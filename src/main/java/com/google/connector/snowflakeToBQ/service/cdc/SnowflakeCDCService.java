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

package com.google.connector.snowflakeToBQ.service.cdc;

import static com.google.connector.snowflakeToBQ.util.PropertyManager.OUTPUT_FORMATTER1;

import com.google.connector.snowflakeToBQ.config.cdc.CDCJobScheduler;
import com.google.connector.snowflakeToBQ.entity.CDCJobConfigData;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.mapper.MigrateRequestMapper;
import com.google.connector.snowflakeToBQ.model.datadto.BigQueryDetailsDataDTO;
import com.google.connector.snowflakeToBQ.model.datadto.CDCBigQueryDetailsDataDTO;
import com.google.connector.snowflakeToBQ.model.request.SFCDCRequestDTO;
import com.google.connector.snowflakeToBQ.model.response.CDCJobTriggerResponse;
import com.google.connector.snowflakeToBQ.service.BigQueryOperationsService;
import com.google.connector.snowflakeToBQ.service.CDCJobConfigDataService;
import com.google.connector.snowflakeToBQ.service.SnowflakesService;
import com.google.connector.snowflakeToBQ.util.PropertyManager;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

@Service
public class SnowflakeCDCService {
  private static final Logger log = LoggerFactory.getLogger(SnowflakeCDCService.class);
  private static final String REQUEST_LOG_ID = "requestLogId";
  final CDCJobScheduler cdcTaskScheduler;
  final CDCJobConfigDataService cdcJobConfigDataService;

  final SnowflakesService snowflakesService;

  final BigQueryOperationsService bigQueryOperationsService;
  private final ConcurrentHashMap<String, ScheduledFuture<?>> scheduledTasks =
      new ConcurrentHashMap<>();

  @Autowired
  public SnowflakeCDCService(
      CDCJobScheduler cdcTaskScheduler,
      CDCJobConfigDataService cdcJobConfigDataService,
      SnowflakesService snowflakesService,
      BigQueryOperationsService bigQueryOperationsService) {
    this.cdcTaskScheduler = cdcTaskScheduler;
    this.cdcJobConfigDataService = cdcJobConfigDataService;
    this.snowflakesService = snowflakesService;
    this.bigQueryOperationsService = bigQueryOperationsService;
  }

  /**
   * Method to start the CDC process for a table. It basically starts a separate scheduler for each
   * table and keeps on polling on a given time, until stopped. Table name becomes the scheduler
   * name, which can be used to stop the scheduler when it's not required. Scheduler will perform
   * the below tasks 1: Send a request to Snowflake to unload the stream data from Snowflake, stream
   * name would be the source table as request parameters. 2: Load that data into the table created
   * at run time with the same name as source table. 3: Execute the merge script to merge this data
   * to base table. User provides the script. 4: Delete the data from GCS. 5: Repeat the steps from
   * 1-4 ensuring that no back of any steps is pending.
   *
   * @param sfcdcRequestDTO DTO which contains the value provided by user as a part of rest request
   *     input.
   * @return @{@link List} of the scheduled taskIds.
   */
  public List<CDCJobTriggerResponse> triggerCDCForInputTables(SFCDCRequestDTO sfcdcRequestDTO) {

    List<CDCJobConfigData> cdcJobConfigDataList = saveCDCJobsToConfigDatabase(sfcdcRequestDTO);
    List<CDCJobTriggerResponse> returnResponse = new ArrayList<>();
    String requestLogId = MDC.get(REQUEST_LOG_ID);

    for (CDCJobConfigData cdcJobConfigData : cdcJobConfigDataList) {
      String taskId = cdcJobConfigData.getSourceTableName() + "-" + UUID.randomUUID();
      Runnable task =
          () -> {
            String mdcTempRequestLogId = requestLogId + "--" + UUID.randomUUID();
            MDC.put(REQUEST_LOG_ID, mdcTempRequestLogId);

            // Marking the start of processing to make sure that first execution of the scheduler is
            // scheduled, setting the task id for further processing and tracking
            if (StringUtils.isBlank(cdcJobConfigData.getTaskId())) {
              cdcJobConfigData.setTaskId(taskId);
              cdcJobConfigData.setCDCStarted(true);
              cdcJobConfigDataService.saveApplicationConfigDataService(cdcJobConfigData);
            } else if (shouldSkipTask(cdcJobConfigData.getTaskId())) {
              log.info(
                  "Skipping the next execution of taskId:{} as some of previous execution's tasks are not completed",
                  taskId);
              return;
            }
            // Execute the step to get the CDC data from Snowflake via copy-into command to GCS
            getCDCDataFromSnowflakeToGCS(cdcJobConfigData, taskId);

            // Load the CDC data into Stage table of BigQuery
            loadCDCDataToBigQueryStageTable(cdcJobConfigData);

            MDC.remove(mdcTempRequestLogId);
          };

      CronTrigger cronTrigger = new CronTrigger(sfcdcRequestDTO.getCronExpression());

      ScheduledFuture<?> scheduledTask =
          cdcTaskScheduler.taskScheduler().schedule(task, cronTrigger);

      scheduledTasks.put(taskId, scheduledTask);

      CDCJobTriggerResponse cdcJobTriggerResponse = new CDCJobTriggerResponse();
      cdcJobTriggerResponse.setTableName(cdcJobConfigData.getSourceTableName());
      cdcJobTriggerResponse.setBaseTableName(cdcJobConfigData.getBaseTableNameInBQ());
      cdcJobTriggerResponse.setTaskId(taskId);

      returnResponse.add(cdcJobTriggerResponse);

      log.info(
          "Started CDC task scheduler for table: {}, base table:{}, taskId for the scheduler task is:{} ",
          cdcJobConfigData.getSourceTableName(),
          cdcJobConfigData.getBaseTableNameInBQ(),
          taskId);
    }
    log.info("Scheduled taskIds are :{}", scheduledTasks.keySet());
    return returnResponse;
  }

  /**
   * Method to stop CDC tasks which were scheduled based on other method execution by user. TaskId
   * uniquely identifies the scheduled task.
   *
   * @param taskIds taskIds in comma separate format.
   * @return @{@link String} response to caller about the total/running/stopped tasks
   */
  public String stopCDCTasksForTables(String taskIds) {
    log.info("TaskId(s) received to stop:{}", taskIds);
    int currentScheduledTasks = scheduledTasks.size();
    try {
      String[] taskIdsTemp = taskIds.split(",");
      for (String taskId : taskIdsTemp) {
        ScheduledFuture<?> scheduledTask = scheduledTasks.get(taskId);
        if (scheduledTask != null) {
          scheduledTask.cancel(true);
          scheduledTasks.remove(taskId);
          log.info("Stopped CDC scheduler task for taskId: " + taskId);
        } else {
          log.info("No CDC task found for taskId: " + taskId);
        }
      }
    } catch (Exception e) {
      log.error("Error while stopping the scheduled tasks for CDC task schedulers");
      return String.format(
          "Error encountered while stopping tasks. Total CDC Tasks/Jobs scheduled::%d, Tasks stopped:%d, current running tasks:%d",
          currentScheduledTasks,
          currentScheduledTasks - scheduledTasks.size(),
          scheduledTasks.size());
    }
    return String.format(
        "Total CDC Tasks/Jobs scheduled::%d, Tasks stopped:%d, current running tasks:%d",
        currentScheduledTasks,
        currentScheduledTasks - scheduledTasks.size(),
        scheduledTasks.size());
  }

  /**
   * This is the helper method to execute the step for bringing the CDC data to GCS using copy-into
   * command
   *
   * @param cdcJobConfigData config data related CDC job
   * @param taskId Task If of the scheduler which is executing the CDC flow.
   */
  private void getCDCDataFromSnowflakeToGCS(CDCJobConfigData cdcJobConfigData, String taskId) {
    // TODO uncomment the below code
    String snowflakeStatementHandle = "test";
    //                snowflakesService.executeUnloadDataCommand(
    //                    cdcJobConfigData.getTargetTableName(),
    //                    cdcJobConfigData.getSnowflakeStageLocation(),
    //                    cdcJobConfigData.getSnowflakeFileFormatValue());
    log.info(
        "Snowflake statement handle:: {}, for table name:: {}, taskId:{}",
        snowflakeStatementHandle,
        cdcJobConfigData.getTargetTableName(),
        taskId);

    // Marking the snowflake unload step complete to save it in database.
    cdcJobConfigData.setDataUnloadedFromSnowflake(true);
    cdcJobConfigData.setSnowflakeStatementHandle(snowflakeStatementHandle);
    cdcJobConfigData.setLastUpdatedTime(
        PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
    cdcJobConfigDataService.saveApplicationConfigDataService(cdcJobConfigData);
  }

  /**
   * Helper method to load the CDC data to stage table of BigQuery.
   *
   * @param cdcJobConfigData config data related CDC job
   */
  private void loadCDCDataToBigQueryStageTable(CDCJobConfigData cdcJobConfigData) {

    CDCBigQueryDetailsDataDTO cdcBigQueryDetailsDataDTO =
        MigrateRequestMapper.cdcJobRequestToBigQueryDetailDataDto(cdcJobConfigData);

    cdcBigQueryDetailsDataDTO.setWriteDisposition(
        BigQueryDetailsDataDTO.WriteDispositionValue.WRITE_TRUNCATE);
    cdcBigQueryDetailsDataDTO.setBigqueryJobNamePrefix(PropertyManager.CDC_JOB_NAME_PREFIX);

    try {
      // Additional metadata column received as a part of CDC row from Snowflake.
      additionalCDCColumns(cdcBigQueryDetailsDataDTO);
      boolean loadResponse =
          bigQueryOperationsService.loadDataUsingWriteDisposition(cdcBigQueryDetailsDataDTO);

      if (!loadResponse) {
        throw new SnowflakeConnectorException(
            String.format("%s, Error: while loading table", cdcJobConfigData.getTargetTableName()),
            0);
      }
    } catch (Exception e) {
      log.error("Error while loading data in the table:{} ", cdcJobConfigData.getTargetTableName());
      cdcJobConfigData.setLastUpdatedTime(
          PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));
      cdcJobConfigDataService.saveApplicationConfigDataService(cdcJobConfigData);
      throw new SnowflakeConnectorException(
          String.format("%s, Error:%s", cdcJobConfigData.getTargetTableName(), e.getMessage()), 0);
    }

    cdcJobConfigData.setDataLoadedInBQ(true);
    cdcJobConfigData.setLastUpdatedTime(
        PropertyManager.getDateInDesiredFormat(LocalDateTime.now(), OUTPUT_FORMATTER1));

    cdcJobConfigDataService.saveApplicationConfigDataService(cdcJobConfigData);
  }

  /*
  Additional metadata columns needed to be added in the table.
   */
  private static void additionalCDCColumns(CDCBigQueryDetailsDataDTO cdcBigQueryDetailsDataDTO) {
    Map<String, BigQueryDetailsDataDTO.Datatypes> additionalColumnMetaData = new HashMap<>();
    additionalColumnMetaData.put("TEST", BigQueryDetailsDataDTO.Datatypes.STRING);
    additionalColumnMetaData.put("METADATA_ISUPDATE", BigQueryDetailsDataDTO.Datatypes.BOOL);
    additionalColumnMetaData.put("METADATA_ROW_ID", BigQueryDetailsDataDTO.Datatypes.STRING);
    additionalColumnMetaData.put("DATA_UNLOADING_TIME", BigQueryDetailsDataDTO.Datatypes.STRING);
    cdcBigQueryDetailsDataDTO.setColumnMetadata(additionalColumnMetaData);
  }

  /** Helper method to save the cdc config data to the database. */
  private List<CDCJobConfigData> saveCDCJobsToConfigDatabase(SFCDCRequestDTO sfcdcRequestDTO) {

    List<CDCJobConfigData> cdcJobConfigDataList =
        MigrateRequestMapper.getCDCJobConfigFromSFCDCRequestDTO(sfcdcRequestDTO);
    return cdcJobConfigDataService.saveAllApplicationConfigDataServices(cdcJobConfigDataList);
  }

  /**
   * This method will return true if either of the tasks "data is not loaded in bigquery" or "data
   * unload from Snowflake is not complete". Which simple means that next execution of the task
   * should be skipped. It will also return true is the tasks are stopped.
   *
   * @param taskId task Id of the task saved before starting the job execution.
   * @return true- task should be skipped, false means task should not be skipped
   */
  private boolean shouldSkipTask(String taskId) {
    CDCJobConfigData cdcJobConfigDataTemp = cdcJobConfigDataService.findByColumnName(taskId);

    if (cdcJobConfigDataTemp.isCDCStopped()) {
      return true;
    } else
      return cdcJobConfigDataTemp.isCDCStarted()
          && (!cdcJobConfigDataTemp.isDataLoadedInBQ()
              || !cdcJobConfigDataTemp.isDataUnloadedFromSnowflake());
  }
}
