/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.iceberg.service.cleanup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIcebergCleanupFailureHandling {

  @Test
  void testBulkFailureEscapesWithoutLocalRetry() {
    SupportsBulkOperations io = mock(SupportsBulkOperations.class);
    AtomicInteger calls = new AtomicInteger();
    doAnswer(
            ignored -> {
              calls.incrementAndGet();
              throw new BulkDeletionFailureException(1);
            })
        .when(io)
        .deleteFiles(any());

    IcebergCleanupManager manager =
        new IcebergCleanupManager(
            mock(IcebergCleanupJobStore.class), new IcebergConfig(new HashMap<>()));
    try {
      Assertions.assertThrows(
          BulkDeletionFailureException.class, () -> manager.deleteAll(io, List.of("a")));
      Assertions.assertEquals(1, calls.get());
    } finally {
      manager.close();
    }
  }

  @Test
  void testOrdinaryBulkFailureEscapesWithoutLocalRetry() {
    SupportsBulkOperations io = mock(SupportsBulkOperations.class);
    AtomicInteger calls = new AtomicInteger();
    IllegalStateException expected = new IllegalStateException("permanent failure");
    doAnswer(
            ignored -> {
              calls.incrementAndGet();
              throw expected;
            })
        .when(io)
        .deleteFiles(any());

    IcebergCleanupManager manager =
        new IcebergCleanupManager(
            mock(IcebergCleanupJobStore.class), new IcebergConfig(new HashMap<>()));
    try {
      Assertions.assertSame(
          expected,
          Assertions.assertThrows(
              IllegalStateException.class, () -> manager.deleteAll(io, List.of("a"))));
      Assertions.assertEquals(1, calls.get());
    } finally {
      manager.close();
    }
  }

  @Test
  void testNonBulkMissingPathDoesNotSkipRemainingPaths() {
    FileIO io = mock(FileIO.class);
    List<String> attempted = new CopyOnWriteArrayList<>();
    doAnswer(
            invocation -> {
              String path = invocation.getArgument(0);
              attempted.add(path);
              if ("missing".equals(path)) {
                throw new NotFoundException("already absent");
              }
              return null;
            })
        .when(io)
        .deleteFile(anyString());

    IcebergCleanupManager manager =
        new IcebergCleanupManager(
            mock(IcebergCleanupJobStore.class), new IcebergConfig(new HashMap<>()));
    try {
      Assertions.assertDoesNotThrow(() -> manager.deleteAll(io, List.of("missing", "present")));
      Assertions.assertEquals(List.of("missing", "present"), attempted);
    } finally {
      manager.close();
    }
  }

  @Test
  void testNonBulkFailureEscapes() {
    FileIO io = mock(FileIO.class);
    IllegalStateException expected = new IllegalStateException("delete denied");
    doAnswer(
            ignored -> {
              throw expected;
            })
        .when(io)
        .deleteFile(anyString());

    IcebergCleanupManager manager =
        new IcebergCleanupManager(
            mock(IcebergCleanupJobStore.class), new IcebergConfig(new HashMap<>()));
    try {
      Assertions.assertSame(
          expected,
          Assertions.assertThrows(
              IllegalStateException.class, () -> manager.deleteAll(io, List.of("a"))));
    } finally {
      manager.close();
    }
  }

  @Test
  void testDeleteAllWaitsForSiblingBatchesBeforePropagatingFailure() throws Exception {
    CountDownLatch siblingStarted = new CountDownLatch(1);
    CountDownLatch releaseSibling = new CountDownLatch(1);
    FileIO io = mock(FileIO.class, withSettings().extraInterfaces(SupportsBulkOperations.class));
    doAnswer(
            invocation -> {
              String file = invocation.<Iterable<String>>getArgument(0).iterator().next();
              if ("failure".equals(file)) {
                Assertions.assertTrue(siblingStarted.await(5, TimeUnit.SECONDS));
                throw new AssertionError("test batch failure");
              }
              siblingStarted.countDown();
              Assertions.assertTrue(releaseSibling.await(5, TimeUnit.SECONDS));
              return null;
            })
        .when((SupportsBulkOperations) io)
        .deleteFiles(any());

    IcebergCleanupManager manager = managerWithTwoDeleteThreads();
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      Future<?> cleanup = caller.submit(() -> manager.deleteAll(io, List.of("failure", "slow")));
      Assertions.assertTrue(siblingStarted.await(5, TimeUnit.SECONDS));
      Assertions.assertFalse(cleanup.isDone());

      releaseSibling.countDown();
      Assertions.assertThrows(ExecutionException.class, cleanup::get);
    } finally {
      releaseSibling.countDown();
      caller.shutdownNow();
      manager.close();
    }
  }

  @Test
  void testInterruptionDrainsSiblingBatchesAndRestoresInterrupt() throws Exception {
    CountDownLatch batchesStarted = new CountDownLatch(2);
    CountDownLatch releaseBatches = new CountDownLatch(1);
    FileIO io = mock(FileIO.class, withSettings().extraInterfaces(SupportsBulkOperations.class));
    doAnswer(
            ignored -> {
              batchesStarted.countDown();
              Assertions.assertTrue(releaseBatches.await(5, TimeUnit.SECONDS));
              return null;
            })
        .when((SupportsBulkOperations) io)
        .deleteFiles(any());

    IcebergCleanupManager manager = managerWithTwoDeleteThreads();
    AtomicReference<RuntimeException> failure = new AtomicReference<>();
    AtomicBoolean interrupted = new AtomicBoolean();
    Thread caller =
        new Thread(
            () -> {
              try {
                manager.deleteAll(io, List.of("a", "b"));
              } catch (RuntimeException e) {
                failure.set(e);
                interrupted.set(Thread.currentThread().isInterrupted());
              }
            });
    try {
      caller.start();
      Assertions.assertTrue(batchesStarted.await(5, TimeUnit.SECONDS));
      caller.interrupt();
      caller.join(100L);
      Assertions.assertTrue(caller.isAlive(), "Caller must drain sibling batches before returning");

      releaseBatches.countDown();
      caller.join(5_000L);
      Assertions.assertFalse(caller.isAlive());
      Assertions.assertNotNull(failure.get());
      Assertions.assertTrue(interrupted.get());
    } finally {
      releaseBatches.countDown();
      caller.interrupt();
      caller.join(5_000L);
      manager.close();
    }
  }

  private static IcebergCleanupManager managerWithTwoDeleteThreads() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("async-cleanup.delete-batch-size", "1");
    properties.put("async-cleanup.delete-threads", "2");
    return new IcebergCleanupManager(
        mock(IcebergCleanupJobStore.class), new IcebergConfig(properties));
  }
}
