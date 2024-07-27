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

package com.google.connector.snowflakeToBQ.model.datadto;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * Class which will be used by services and hold the data related to BigQuery and helps in BigQuery
 * related operations
 */
@Setter
@Getter
public class BigQueryDetailsDataDTO {

  private long uniqueIdentifier;
  private String projectId;
  private String datasetId;
  private String tableName;
  private String bucketName;
  private String gcsDDLFilePath;
  private String snowflakeDataUnloadGCSPath;
  private String bqLoadFileFormat;
  private String location;
  private String bigqueryJobNamePrefix;

  private Map<String, Datatypes> columnMetadata;

  private WriteDispositionValue writeDisposition = WriteDispositionValue.NA;

  public enum WriteDispositionValue {
    WRITE_TRUNCATE,
    WRITE_APPEND,
    WRITE_EMPTY,
    NA
  }

  public enum Datatypes {
    STRING,
    INT64,
    TIMESTAMP,
    BOOL,
    FLOAT64,
    NUMERIC,
    BIGNUMERIC,
    DATE
  }

  @Override
  public String toString() {
    return "BigQueryDetailsDataDTO{"
        + "uniqueIdentifier="
        + uniqueIdentifier
        + ", projectId='"
        + projectId
        + '\''
        + ", datasetId='"
        + datasetId
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", bucketName='"
        + bucketName
        + '\''
        + ", gcsDDLFilePath='"
        + gcsDDLFilePath
        + '\''
        + ", snowflakeDataUnloadGCSPath='"
        + snowflakeDataUnloadGCSPath
        + '\''
        + ", bqLoadFileFormat='"
        + bqLoadFileFormat
        + '\''
        + ", location='"
        + location
        + '\''
        + ", bigqueryJobNamePrefix='"
        + bigqueryJobNamePrefix
        + '\''
        + ", columnMetadata="
        + columnMetadata
        + ", writeDisposition="
        + writeDisposition
        + '}';
  }
}
