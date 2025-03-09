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

import static org.mockito.Mockito.*;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.longrunning.OperationSnapshot;
import com.google.api.gax.retrying.RetryingFuture;
import com.google.connector.snowflakeToBQ.base.AbstractTestBase;
import com.google.protobuf.Empty;
import com.google.connector.snowflakeToBQ.exception.SnowflakeConnectorException;
import com.google.connector.snowflakeToBQ.model.datadto.STSDataDTO;
import com.google.connector.snowflakeToBQ.service.Instancecreator.STSInstanceCreator;
import com.google.storagetransfer.v1.proto.StorageTransferServiceClient;
import com.google.storagetransfer.v1.proto.TransferProto.CreateTransferJobRequest;
import com.google.storagetransfer.v1.proto.TransferProto.RunTransferJobRequest;
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob;
import com.google.storagetransfer.v1.proto.TransferTypes.TransferOperation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import org.springframework.test.context.TestPropertySource;

/**
 * Unit test class for the S3ToBigQueryServiceUsingSTS class.
 * This class contains test cases for the transfer of files from S3 to GCS
 * using the Storage Transfer Service (STS) APIs, simulating various scenarios.
 */

public class S3ToBigQueryServiceUsingSTSTest extends AbstractTestBase {

  @Autowired private S3ToBigQueryServiceUsingSTS s3ToBigQueryServiceUsingSTS;

  @MockBean private STSInstanceCreator stsInstanceCreator;

  private StorageTransferServiceClient stsMock;
  @Mock private OperationFuture<Empty, TransferOperation> mockOperationFuture;
  @Mock private RetryingFuture<OperationSnapshot> mockOperationSnapshotRetyringFuture;

  @Mock private OperationSnapshot mockOperationObject;

  @Value("${aws.access.key.id}")
  private String awsAccessKeyId;

  @Value("${aws.secret.access.id}")
  private String awsSecretAccessId;

  /**
   * Setup method that runs before each test.
   * Initializes mocks and sets up the StorageTransferServiceClient mock.
   */
  @Before
  public void setUp() {
    stsMock = mock(StorageTransferServiceClient.class);
    when(stsInstanceCreator.getStorageTransferServiceClient()).thenReturn(stsMock);
  }

  /**
   * Helper method to create a mock STSDataDTO.
   * @return a mock STSDataDTO instance
   */
  private STSDataDTO getSTSDataDTO() {
    STSDataDTO stsDataDTO = new STSDataDTO();
    stsDataDTO.setTableName("source_table");
    stsDataDTO.setProjectId("my_project");
    stsDataDTO.setDatasetId("my_dataset");
    stsDataDTO.setSnowflakeDataUnloadGCSPath("snowflake_unload_path");
    stsDataDTO.setBqLoadFileFormat("CSV");
    stsDataDTO.setTempGCSBucketNameForS3Data("temp-bucket-name");
    return stsDataDTO;
  }

  /**
   * Test case for transferring files from S3 to GCS with a successful operation.
   * Verifies that no exception is thrown when the operation is completed successfully.
   * @throws InterruptedException if the thread is interrupted during execution
   * @throws ExecutionException if an exception occurs during the execution of the operation
   * @throws TimeoutException if the operation times out
   */
  @Test
  public void transferS3FilesToGCS()
      throws InterruptedException, ExecutionException, TimeoutException {
    STSDataDTO stsDataDTO = getSTSDataDTO();

    when(stsMock.createTransferJob(any(CreateTransferJobRequest.class)))
        .thenReturn(TransferJob.newBuilder().setName("name").build());

    when(stsMock.runTransferJobAsync(any(RunTransferJobRequest.class)))
        .thenReturn(mockOperationFuture);

    when(mockOperationFuture.getPollingFuture()).thenReturn(mockOperationSnapshotRetyringFuture);

    when(mockOperationSnapshotRetyringFuture.get(anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(mockOperationObject);

    when(mockOperationObject.isDone()).thenReturn(true);
    s3ToBigQueryServiceUsingSTS.transferS3FilesToGCS(stsDataDTO);
  }

  /**
   * Test case for the scenario where the transfer job's polling future returns null.
   * Verifies that a SnowflakeConnectorException is thrown when the polling future is null.
   * @throws InterruptedException if the thread is interrupted during execution
   * @throws ExecutionException if an exception occurs during the execution of the operation
   * @throws TimeoutException if the operation times out
   */
  @Test
  public void transferS3FilesToGCSOperationNull()
      throws InterruptedException, ExecutionException, TimeoutException {
    STSDataDTO stsDataDTO = getSTSDataDTO();

    when(stsMock.createTransferJob(any(CreateTransferJobRequest.class)))
        .thenReturn(TransferJob.newBuilder().setName("name").build());

    when(stsMock.runTransferJobAsync(any(RunTransferJobRequest.class)))
        .thenReturn(mockOperationFuture);

    when(mockOperationFuture.getPollingFuture()).thenReturn(mockOperationSnapshotRetyringFuture);

    when(mockOperationSnapshotRetyringFuture.get(anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(null);

    Assert.assertThrows(
        SnowflakeConnectorException.class,
        () -> s3ToBigQueryServiceUsingSTS.transferS3FilesToGCS(stsDataDTO));
  }

  /**
   * Test case for the scenario where the operation is not done.
   * Verifies that a SnowflakeConnectorException is thrown when the operation is not completed.
   * @throws InterruptedException if the thread is interrupted during execution
   * @throws ExecutionException if an exception occurs during the execution of the operation
   * @throws TimeoutException if the operation times out
   */
  @Test
  public void transferS3FilesToGCSJobNotDone()
      throws InterruptedException, ExecutionException, TimeoutException {
    STSDataDTO stsDataDTO = getSTSDataDTO();

    when(stsMock.createTransferJob(any(CreateTransferJobRequest.class)))
        .thenReturn(TransferJob.newBuilder().setName("name").build());

    when(stsMock.runTransferJobAsync(any(RunTransferJobRequest.class)))
        .thenReturn(mockOperationFuture);

    when(mockOperationFuture.getPollingFuture()).thenReturn(mockOperationSnapshotRetyringFuture);

    when(mockOperationSnapshotRetyringFuture.get(anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(mockOperationObject);

    when(mockOperationObject.isDone()).thenReturn(false);
    Assert.assertThrows(
        SnowflakeConnectorException.class,
        () -> s3ToBigQueryServiceUsingSTS.transferS3FilesToGCS(stsDataDTO));
  }

  /**
   * Test case to simulate an exception scenario where ExecutionException occurs.
   * Verifies that a SnowflakeConnectorException is thrown when ExecutionException occurs.
   * @throws InterruptedException if the thread is interrupted during execution
   * @throws ExecutionException if an exception occurs during the execution of the operation
   * @throws TimeoutException if the operation times out
   */
  @Test
  public void testSTSJObExceptionScenario_ExecutionException()
      throws InterruptedException, ExecutionException, TimeoutException {
    STSDataDTO stsDataDTO = getSTSDataDTO();
    stsDataDTO.setSnowflakeDataUnloadGCSPath("snowflake_unload_path/Dataunload");

    when(stsMock.createTransferJob(any(CreateTransferJobRequest.class)))
        .thenReturn(TransferJob.newBuilder().setName("transfer-123").build());

    when(stsMock.runTransferJobAsync(any(RunTransferJobRequest.class)))
        .thenReturn(mockOperationFuture);
    when(mockOperationFuture.getPollingFuture()).thenReturn(mockOperationSnapshotRetyringFuture);
    when(mockOperationSnapshotRetyringFuture.get(anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenThrow(new ExecutionException("Mocked execution failure", new RuntimeException()));

    // Verify exception is thrown when calling transfer
    Assert.assertThrows(
        SnowflakeConnectorException.class,
        () -> s3ToBigQueryServiceUsingSTS.transferS3FilesToGCS(stsDataDTO));
  }

  /**
   * Test case to simulate an exception scenario where TimeoutException occurs.
   * Verifies that a SnowflakeConnectorException is thrown when TimeoutException occurs.
   * @throws InterruptedException if the thread is interrupted during execution
   * @throws ExecutionException if an exception occurs during the execution of the operation
   * @throws TimeoutException if the operation times out
   */
  @Test
  public void testSTSJObExceptionScenario_TimeoutException()
      throws InterruptedException, ExecutionException, TimeoutException {
    STSDataDTO stsDataDTO = getSTSDataDTO();
    stsDataDTO.setSnowflakeDataUnloadGCSPath("snowflake_unload_path/Dataunload");

    when(stsMock.createTransferJob(any(CreateTransferJobRequest.class)))
        .thenReturn(TransferJob.newBuilder().setName("transfer-123").build());

    when(stsMock.runTransferJobAsync(any(RunTransferJobRequest.class)))
        .thenReturn(mockOperationFuture);
    when(mockOperationFuture.getPollingFuture()).thenReturn(mockOperationSnapshotRetyringFuture);
    when(mockOperationSnapshotRetyringFuture.get(anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenThrow(new TimeoutException("Mocked timeout exception"));

    // Verify exception is thrown when calling transfer
    Assert.assertThrows(
        SnowflakeConnectorException.class,
        () -> s3ToBigQueryServiceUsingSTS.transferS3FilesToGCS(stsDataDTO));
  }

  /**
   * Test case to simulate an exception scenario where CancellationException occurs.
   * Verifies that a SnowflakeConnectorException is thrown when CancellationException occurs.
   * @throws InterruptedException if the thread is interrupted during execution
   * @throws ExecutionException if an exception occurs during the execution of the operation
   * @throws TimeoutException if the operation times out
   */
  @Test
  public void testSTSJObExceptionScenario_CancellationException()
      throws InterruptedException, ExecutionException, TimeoutException {
    STSDataDTO stsDataDTO = getSTSDataDTO();
    stsDataDTO.setSnowflakeDataUnloadGCSPath("snowflake_unload_path/Dataunload");

    when(stsMock.createTransferJob(any(CreateTransferJobRequest.class)))
        .thenReturn(TransferJob.newBuilder().setName("transfer-123").build());

    when(stsMock.runTransferJobAsync(any(RunTransferJobRequest.class)))
        .thenReturn(mockOperationFuture);
    when(mockOperationFuture.getPollingFuture()).thenReturn(mockOperationSnapshotRetyringFuture);
    when(mockOperationSnapshotRetyringFuture.get(anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenThrow(new CancellationException("Mocked cancellation exception"));

    // Verify exception is thrown when calling transfer
    Assert.assertThrows(
        SnowflakeConnectorException.class,
        () -> s3ToBigQueryServiceUsingSTS.transferS3FilesToGCS(stsDataDTO));
  }

  /**
   * Test case to simulate an exception scenario where InterruptedException occurs.
   * Verifies that a SnowflakeConnectorException is thrown when InterruptedException occurs.
   * @throws InterruptedException if the thread is interrupted during execution
   * @throws ExecutionException if an exception occurs during the execution of the operation
   * @throws TimeoutException if the operation times out
   */
  @Test
  public void testSTSJObExceptionScenario_InterruptedException()
      throws InterruptedException, ExecutionException, TimeoutException {
    STSDataDTO stsDataDTO = getSTSDataDTO();
    stsDataDTO.setSnowflakeDataUnloadGCSPath("snowflake_unload_path/Dataunload");

    when(stsMock.createTransferJob(any(CreateTransferJobRequest.class)))
        .thenReturn(TransferJob.newBuilder().setName("transfer-123").build());

    when(stsMock.runTransferJobAsync(any(RunTransferJobRequest.class)))
        .thenReturn(mockOperationFuture);
    when(mockOperationFuture.getPollingFuture()).thenReturn(mockOperationSnapshotRetyringFuture);
    when(mockOperationSnapshotRetyringFuture.get(anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenThrow(new InterruptedException("Mocked interrupted exception"));

    // Verify exception is thrown when calling transfer
    Assert.assertThrows(
        SnowflakeConnectorException.class,
        () -> s3ToBigQueryServiceUsingSTS.transferS3FilesToGCS(stsDataDTO));
  }
}
