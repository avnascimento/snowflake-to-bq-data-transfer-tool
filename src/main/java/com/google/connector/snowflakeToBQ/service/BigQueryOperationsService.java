/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.connector.snowflakeToBQ.service;

import static com.google.connector.snowflakeToBQ.util.ErrorCode.*;

import com.google.cloud.bigquery.*;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.datadto.BigQueryDetailsDataDTO;
import com.google.connector.snowflakeToBQ.model.datadto.CDCBigQueryDetailsDataDTO;
import com.google.connector.snowflakeToBQ.service.Instancecreator.BigQueryInstanceCreator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.connector.snowflakeToBQ.service.bigqueryjoboptions.LoadJobFactory;
import com.google.connector.snowflakeToBQ.service.bigqueryjoboptions.LoadOption;
import com.google.connector.snowflakeToBQ.util.PropertyManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Class to provide methods which help in performing bigquery related operations like create table,
 * load job etc.
 */
@Service
public class BigQueryOperationsService {
  private static final Logger log = LoggerFactory.getLogger(BigQueryOperationsService.class);
  final BigQueryInstanceCreator bigQueryInstanceCreator;
  final LoadJobFactory loadJobFactory;

  public BigQueryOperationsService(
      BigQueryInstanceCreator bigQueryInstanceCreator, LoadJobFactory loadJobFactory) {
    this.bigQueryInstanceCreator = bigQueryInstanceCreator;
    this.loadJobFactory = loadJobFactory;
  }

  /**
   * Method to perform the load job in BigQuery. This method has a check to make sure table is
   * already created before it loads the data, it could be created by user manually or as a part of
   * another flow of this application
   *
   * @param bigqueryDetailsDto required parameter for executing the load job.
   * @return @boolean status
   */
  public boolean loadDataToExistingTable(BigQueryDetailsDataDTO bigqueryDetailsDto) {

    TableId tableId =
        TableId.of(
            bigqueryDetailsDto.getProjectId(),
            bigqueryDetailsDto.getDatasetId(),
            bigqueryDetailsDto.getTableName());

    // Validating if the table for which load job is to perform exists or not
    // This logic is important because table gets used to get the schema and same is used during the
    // load job. This tool also supports CSV load hence providing schema is important.
    if (!isTableExists(bigqueryDetailsDto)) {
      log.error(
          "Error Message:{},Error Code:{}, table name:{}",
          TABLE_NOT_EXISTS.getMessage(),
          TABLE_NOT_EXISTS.getErrorCode(),
          bigqueryDetailsDto.getTableName());
      throw new SnowflakeConnectorException(
          TABLE_NOT_EXISTS.getMessage(), TABLE_NOT_EXISTS.getErrorCode());
    }
    // fetching the schema of the table
    Schema tableSchema =
        bigQueryInstanceCreator.getBigQueryClient().getTable(tableId).getDefinition().getSchema();

    return loadJob(bigqueryDetailsDto, tableSchema);
  }

  /**
   * This method executes the bigquery load job using write truncate disposition. It deletes the
   * existing data in the table and inserts new one based on the GCS file and load job configuration
   *
   * @param <T> required parameter for executing the load job. It could be the {@link
   *     BigQueryDetailsDataDTO} or class which inherits it.
   * @return @boolean status
   */
  public <T extends BigQueryDetailsDataDTO> boolean loadDataUsingWriteDisposition(T detailsDTO) {

    TableId tableId;
    if (detailsDTO instanceof CDCBigQueryDetailsDataDTO) {
      CDCBigQueryDetailsDataDTO cdcBigQueryDetailsDataDTO = (CDCBigQueryDetailsDataDTO) detailsDTO;
      // Here the base table is used for fetching the schema instead of the stream table just to
      // keep the execution simple and process tied to base table. Assumption is in case of any
      // alter in column, same will be applied to based table before bringing the cdc data.
      // Stream table will be assumed as stage table in BigQuery which will get merged to base
      // table.
      tableId =
          TableId.of(
              cdcBigQueryDetailsDataDTO.getProjectId(),
              cdcBigQueryDetailsDataDTO.getDatasetId(),
              cdcBigQueryDetailsDataDTO.getBaseTableNameInBQ());
    } else {
      tableId =
          TableId.of(
              detailsDTO.getProjectId(), detailsDTO.getDatasetId(), detailsDTO.getTableName());
    }
    // This logic is converting additional columns into the @Field object which can be added in
    // bigquery @Schema object. This was needed specifically for CDC approach where there were few
    // additional metadata column were present in the table in Snowflakes
    List<Field> additionalFields =
        detailsDTO.getColumnMetadata().entrySet().stream()
            .map(this::convertToField)
            .collect(Collectors.toList());

    // Use the fields as needed
    // fetching the schema of the table
    Schema tableSchema =
        bigQueryInstanceCreator.getBigQueryClient().getTable(tableId).getDefinition().getSchema();
    // Adding exitsing column first so keep the sequence correct in table as this table gets created
    // at runtime.
    ArrayList<Field> additionalColumnsTemp = new ArrayList<>(tableSchema.getFields());
    // Adding the additional columns in table column list.
    additionalColumnsTemp.addAll(additionalFields);

    return loadJob(detailsDTO, Schema.of(additionalColumnsTemp));
  }

  public boolean loadJob(BigQueryDetailsDataDTO bigqueryDetailsDto, Schema tableSchema) {

    boolean returnValue = false;
    TableId tableId =
        TableId.of(
            bigqueryDetailsDto.getProjectId(),
            bigqueryDetailsDto.getDatasetId(),
            bigqueryDetailsDto.getTableName());

    String sourceURI =
        String.format(
            "gs://%s/%s/*",
            bigqueryDetailsDto.getSnowflakeDataUnloadGCSPath(), bigqueryDetailsDto.getTableName());

    // Getting the appropriate loadjobconfiguration object based on the csv format received in the
    // request.
    // It could be CSV, Parquet etc.

    LoadJobConfiguration loadConfig =
        loadJobFactory
            .createService(
                LoadOption.valueOf(bigqueryDetailsDto.getBqLoadFileFormat()), tableSchema)
            .createLoadJob(tableId, sourceURI, getWriteDisposition(bigqueryDetailsDto));

    JobId jobId =
        JobId.newBuilder()
            .setJob(bigqueryDetailsDto.getBigqueryJobNamePrefix() + UUID.randomUUID())
            .setLocation(
                StringUtils.isBlank(bigqueryDetailsDto.getLocation())
                    ? "us"
                    : bigqueryDetailsDto.getLocation())
            .build();

    try {
      Job loadJob =
          bigQueryInstanceCreator
              .getBigQueryClient()
              .create(JobInfo.newBuilder(loadConfig).setJobId(jobId).build());
      // Waiting for job to finish, no options has been give so it will wait max 12 hour with
      // unlimited retry attempts, we can add the max timeout and initial delay later based on the
      // real world use case
      loadJob = loadJob.waitFor();

      if (loadJob == null) {
        log.error(
            "Error executing BigQuery load job for JobId::{} and application's row:{}, return load job object is null",
            jobId.getJob(),
            bigqueryDetailsDto.getUniqueIdentifier());
      } else if (loadJob.getStatus().getError() == null) {
        returnValue = true;
        log.info("Data loaded successfully.");
      } else {
        log.error("Error executing BigQuery load job: {}", loadJob.getStatus().getError());
      }
    } catch (Exception e) {
      log.error(
          "Error executing BigQuery load job for JobId::{} and application's row:{}, Error Message:{}\nStack Trace:",
          jobId.getJob(),
          bigqueryDetailsDto.getUniqueIdentifier(),
          e.getMessage(),
          e);
      throw new SnowflakeConnectorException(
          BQ_QUERY_JOB_EXECUTION_ERROR.getMessage(), BQ_QUERY_JOB_EXECUTION_ERROR.getErrorCode());
    }
    return returnValue;
  }

  /**
   * Method to create table in bigquery
   *
   * @param ddl ddl to create the table.
   * @return boolean status
   */
  public boolean createTableUsingDDL(String ddl, String location) {
    log.info("Received ddl for creating table:{}", ddl);
    // Create table using DDL query
    if (!StringUtils.isBlank(ddl) && queryJob(ddl, location)) {
      log.info("Table successfully got created from ddl");
    } else {
      log.error("Failed to create table from the ddl");
      throw new SnowflakeConnectorException(
          TABLE_CREATION_ERROR.getMessage(), TABLE_CREATION_ERROR.getErrorCode());
    }
    return true;
  }

  /**
   * Method to check if the table exists in BigQuery or not.
   *
   * @param bigqueryDetailsDto dto containing the required details like tablename, dataset,
   *     projectId
   * @return true if table exists, false if it does not exist.
   */
  public boolean isTableExists(BigQueryDetailsDataDTO bigqueryDetailsDto) {
    TableId tableIdObj =
        TableId.of(
            bigqueryDetailsDto.getProjectId(),
            bigqueryDetailsDto.getDatasetId(),
            bigqueryDetailsDto.getTableName());
    // Check if the table exists
    Table existingTable = bigQueryInstanceCreator.getBigQueryClient().getTable(tableIdObj);
    return existingTable != null;
  }

  /**
   * Helper method to execute the query in BigQuery
   *
   * @param sql Sql statement as string
   * @return true if this job is in JobStatus.State.DONE state or if it does not exist, false if the
   *     state is not JobStatus.State.DONE
   */
  private boolean queryJob(String sql, String location) {
    boolean jobStatus = false;

    QueryJobConfiguration queryJobConfiguration =
        QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build();
    // creating the jobId
    JobId jobId =
        JobId.newBuilder()
            .setJob(PropertyManager.MIGRATION_JOB_NAME_PREFIX + UUID.randomUUID())
            .setLocation(StringUtils.isBlank(location) ? "us" : location)
            .build();
    // Executing the query job
    try {
      Job queryJob =
          bigQueryInstanceCreator
              .getBigQueryClient()
              .create(JobInfo.newBuilder(queryJobConfiguration).setJobId(jobId).build());

      // waiting for job to finish
      queryJob = queryJob.waitFor();

      // checking if the job has any error
      if (queryJob.getStatus().getError() == null) {
        log.info("Query job executed successful for sql query:{}", sql);
        jobStatus = queryJob.isDone();
      } else {
        log.error(
            "Error query job contains error while executing. Error Message:{},\n Sql Query:{} ",
            queryJob.getStatus().getError(),
            sql);
      }
    } catch (Exception e) {
      log.error(
          "Error while executing query job. Error Message:{},\n Sql Query:{}\nStack Trace: ",
          e.getMessage(),
          sql,
          e);
      throw new SnowflakeConnectorException(
          BQ_QUERY_JOB_EXECUTION_ERROR.getMessage(), BQ_QUERY_JOB_EXECUTION_ERROR.getErrorCode());
    }
    return jobStatus;
  }

  /**
   * Method to map the user defined disposition to the BigQuery Write disposition. Currently user
   * defined displosition name is key same as @{@link JobInfo.WriteDisposition} to keep the mapping
   * logic simple, any discrepancies would lead to errors dueing mapping.
   *
   * @param bigqueryDetailsDto dto containing the required details like tablename, dataset,
   * @return @{@link JobInfo.WriteDisposition}
   */
  private JobInfo.WriteDisposition getWriteDisposition(BigQueryDetailsDataDTO bigqueryDetailsDto) {
    // Get the write disposition from the DTO
    String writeDisposition = bigqueryDetailsDto.getWriteDisposition().name();

    if (writeDisposition.equals("NA")) {
      return null;
    }
    return JobInfo.WriteDisposition.valueOf(writeDisposition);
  }

  /**
   * Helper method to create Field object based on the received map.
   *
   * @param metadata map contains column name as key and its data type as value.
   * @return @{@link Field} object for a column
   */
  private Field convertToField(Map.Entry<String, BigQueryDetailsDataDTO.Datatypes> metadata) {
    StandardSQLTypeName type = mapToStandardSQLType(metadata.getValue());
    return Field.of(metadata.getKey(), type);
  }

  /**
   * Helper method to map the user defined datatype to actual BigQuery @{@link StandardSQLTypeName}
   *
   * @param dataType Datatype provided by user. It's currently an ENUM created in the application.
   *     it can be updated based on the need.
   * @return @{@link StandardSQLTypeName} which are BigQuery compatible
   */
  private StandardSQLTypeName mapToStandardSQLType(BigQueryDetailsDataDTO.Datatypes dataType) {
    switch (dataType) {
      case STRING:
        return StandardSQLTypeName.STRING;
      case BOOL:
        return StandardSQLTypeName.BOOL;
      case INT64:
        return StandardSQLTypeName.INT64;
      case TIMESTAMP:
        return StandardSQLTypeName.TIMESTAMP;
      case FLOAT64:
        return StandardSQLTypeName.FLOAT64;
      case NUMERIC:
        return StandardSQLTypeName.NUMERIC;
      case BIGNUMERIC:
        return StandardSQLTypeName.BIGNUMERIC;
      case DATE:
        return StandardSQLTypeName.DATE;
      default:
        throw new IllegalArgumentException("Unknown data type, no mapping found " + dataType);
    }
  }
}
