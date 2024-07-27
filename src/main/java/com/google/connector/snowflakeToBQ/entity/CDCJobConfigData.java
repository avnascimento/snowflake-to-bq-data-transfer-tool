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

package com.google.connector.snowflakeToBQ.entity;

import java.io.Serializable;
import javax.persistence.*;
import lombok.Getter;
import lombok.Setter;

/** Entity class to represent the current state of the CDC process. */
@Entity
@Setter
@Getter
@Table(name = "cdc_job_data")
public class CDCJobConfigData implements Serializable {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  private String taskId;

  @Column(name = "source_database_name")
  private String sourceDatabaseName;

  @Column(name = "source_schema_name")
  private String sourceSchemaName;

  @Column(name = "source_table_name")
  private String sourceTableName;

  @Column(name = "target_database_name")
  private String targetDatabaseName;

  @Column(name = "target_schema_name")
  private String targetSchemaName;

  @Column(name = "target_table_name")
  private String targetTableName;

  @Column(name = "base_table_name")
  private String baseTableNameInBQ;

  @Column(name = "location")
  String location;

  @Column(name = "snowflake_stage_location")
  private String snowflakeStageLocation;

  @Column(name = "snowflake_file_format")
  private String snowflakeFileFormatValue;

  @Column(name = "snowflake_statement_handle")
  private String snowflakeStatementHandle;

  @Column(name = "is_data_unloaded_from_snowflake")
  private boolean isDataUnloadedFromSnowflake;

  @Column(name = "bq_load_format")
  private String bqLoadFileFormat;

  @Column(name = "is_data_loaded_in_bq")
  private boolean isDataLoadedInBQ;

  @Column(name = "is_cdc_stopped")
  private boolean isCDCStopped;

  @Column(name = "is_cdc_started")
  private boolean isCDCStarted;

  @Column(name = "cron_expression")
  private String cronExpression;

  @Column(name = "created_time")
  private String createdTime;

  @Column(name = "last_updated_time")
  private String lastUpdatedTime;

  @Column(name = "request_log_id")
  private String requestLogId;
}
