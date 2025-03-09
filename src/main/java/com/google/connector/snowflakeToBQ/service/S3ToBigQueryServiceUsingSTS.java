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

import static com.google.connector.snowflakeToBQ.util.ErrorCode.STS_JOB_EXECUTION_ERROR;

import com.google.api.gax.longrunning.OperationSnapshot;
import com.google.api.gax.retrying.RetryingFuture;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.datadto.STSDataDTO;
import com.google.connector.snowflakeToBQ.service.Instancecreator.STSInstanceCreator;
import com.google.storagetransfer.v1.proto.TransferProto.CreateTransferJobRequest;
import com.google.storagetransfer.v1.proto.TransferProto.RunTransferJobRequest;
import com.google.storagetransfer.v1.proto.TransferTypes.*;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Service responsible for transferring data from AWS S3 to Google Cloud Storage (GCS) using Storage
 * Transfer Service (STS).
 *
 * <p>This service creates a transfer job, initiates the transfer from AWS S3 to GCS, and monitors
 * the progress of the transfer job. In case of failures during the transfer process, exceptions are
 * thrown to indicate the error.
 */
@Service
public class S3ToBigQueryServiceUsingSTS {

  private static final Logger log = LoggerFactory.getLogger(S3ToBigQueryServiceUsingSTS.class);

  private static final String JOB_DESCRIPTION_PREFIX = "s3_to_gcs_";
  private static final String ERROR_MESSAGE =
      "Error executing Storage Transfer Service (STS) job:{},error message:{}";

  private final STSInstanceCreator stsInstanceCreator;

  @Value("${aws.access.key.id}")
  private String awsAccessKeyId;

  @Value("${aws.secret.access.id}")
  private String awsSecretAccessId;

  @Value("${sts.timeout.ms}")
  private long timeoutMilliSeconds;

  /**
   * Constructs the service for transferring data from S3 to GCS using STS.
   *
   * @param stsInstanceCreator the instance creator for Storage Transfer Service (STS) client.
   */
  public S3ToBigQueryServiceUsingSTS(STSInstanceCreator stsInstanceCreator) {
    this.stsInstanceCreator = stsInstanceCreator;
  }

  /**
   * Transfers files from AWS S3 to Google Cloud Storage (GCS).
   *
   * <p>This method initiates the process of transferring data from AWS S3 to GCS, monitors the
   * transfer, and handles any exceptions during the transfer job.
   *
   * @param stsDataDTO the data transfer object containing necessary information such as source and
   *     destination details.
   * @throws SnowflakeConnectorException if an error occurs while executing the transfer job.
   */
  public void transferS3FilesToGCS(STSDataDTO stsDataDTO) {
    try {
      if (StringUtils.isEmpty(awsAccessKeyId) || StringUtils.isEmpty(awsSecretAccessId)) {
        log.error(
            "The aws access key and secret access is not provided which is needed for transferring the s3 data to GCS using STS");
        throw new SnowflakeConnectorException(
            STS_JOB_EXECUTION_ERROR.getMessage(), STS_JOB_EXECUTION_ERROR.getErrorCode());
      }
      TransferJob transferJob = createTransferJob(stsDataDTO);
      TransferJob response = createTransferJobInSTS(transferJob);
      log.info("Storage Transfer Job Created: {}", response.getName());

      runAndMonitorTransferJob(response, stsDataDTO);
    } catch (Exception e) {
      log.error(ERROR_MESSAGE, stsDataDTO.getUniqueIdentifier(), e.getMessage(), e);
      throw new SnowflakeConnectorException(
          STS_JOB_EXECUTION_ERROR.getMessage(), STS_JOB_EXECUTION_ERROR.getErrorCode());
    }
  }

  /**
   * Creates a transfer job object for the S3 to GCS data transfer.
   *
   * @param stsDataDTO the data transfer object containing details for the transfer job.
   * @return the created transfer job.
   */
  private TransferJob createTransferJob(STSDataDTO stsDataDTO) {
    return TransferJob.newBuilder()
        .setDescription(
            JOB_DESCRIPTION_PREFIX + stsDataDTO.getTableName() + "_" + System.currentTimeMillis())
        .setProjectId(stsDataDTO.getProjectId())
        .setTransferSpec(buildTransferSpecForJob(stsDataDTO))
        .setStatus(TransferJob.Status.ENABLED)
        .build();
  }

  /**
   * Creates a transfer job in the Storage Transfer Service (STS).
   *
   * @param transferJob the transfer job to be created in STS.
   * @return the created transfer job.
   */
  private TransferJob createTransferJobInSTS(TransferJob transferJob) {
    return stsInstanceCreator
        .getStorageTransferServiceClient()
        .createTransferJob(
            CreateTransferJobRequest.newBuilder().setTransferJob(transferJob).build());
  }

  /**
   * Runs the transfer job and monitors its progress until completion or timeout.
   *
   * @param response the transfer job response.
   * @param stsDataDTO the data transfer object containing details for the transfer job.
   * @throws ExecutionException if an error occurs while executing the transfer job.
   * @throws InterruptedException if the transfer job is interrupted.
   * @throws TimeoutException if the transfer job exceeds the specified timeout.
   */
  private void runAndMonitorTransferJob(TransferJob response, STSDataDTO stsDataDTO)
      throws ExecutionException, InterruptedException, TimeoutException {
    RunTransferJobRequest request =
        RunTransferJobRequest.newBuilder()
            .setJobName(response.getName())
            .setProjectId(stsDataDTO.getProjectId())
            .build();

    RetryingFuture<OperationSnapshot> pollingFuture =
        stsInstanceCreator
            .getStorageTransferServiceClient()
            .runTransferJobAsync(request)
            .getPollingFuture();

    try {
      OperationSnapshot operationSnapshot =
          pollingFuture.get(timeoutMilliSeconds, TimeUnit.MILLISECONDS);

      // Check the operation status
      if (operationSnapshot != null && operationSnapshot.isDone()) {
        log.info("Transfer job {} completed successfully", response.getName());
      } else {
        log.error("Transfer job {} did not complete successfully", response.getName());
        throw new SnowflakeConnectorException(
            STS_JOB_EXECUTION_ERROR.getMessage(), STS_JOB_EXECUTION_ERROR.getErrorCode());
      }

    } catch (CancellationException e) {
      log.error("Transfer Job was cancelled: {}", response.getName(), e);
      throw e;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.error("Exception while waiting for transfer job {} to complete", response.getName());
      throw e;
    }
  }

  /**
   * Builds the transfer specification for the transfer job.
   *
   * <p>This includes the source (AWS S3) and the destination (GCS) configurations.
   *
   * @param stsDataDTO the data transfer object containing the necessary details.
   * @return the transfer specification.
   */
  private TransferSpec buildTransferSpecForJob(STSDataDTO stsDataDTO) {
    String[] parts = stsDataDTO.getSnowflakeDataUnloadGCSPath().split("/", 2);
    String s3Bucket = parts[0];
    String pathExceptBucket =
        Optional.ofNullable(parts.length > 1 ? parts[1] : "")
            .map(value -> value + "/" + stsDataDTO.getTableName() + "/")
            .orElse("");

    return TransferSpec.newBuilder()
        .setAwsS3DataSource(
            AwsS3Data.newBuilder()
                .setBucketName(s3Bucket)
                .setPath(pathExceptBucket)
                .setAwsAccessKey(
                    AwsAccessKey.newBuilder()
                        .setAccessKeyId(awsAccessKeyId)
                        .setSecretAccessKey(awsSecretAccessId)))
        .setGcsDataSink(
            GcsData.newBuilder()
                .setBucketName(stsDataDTO.getTempGCSBucketNameForS3Data())
                .setPath(stsDataDTO.getTableName() + "/"))
        .build();
  }
}
