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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.iceberg.service.cleanup;

import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * Shared cleanup-store test logic exercised against the same relational backend matrix as core
 * metadata service tests. {@link TestJDBCBackend} initializes H2 by default, adds MySQL and
 * PostgreSQL when {@code dockerTest=true}, and truncates all backend tables before each invocation.
 */
abstract class AbstractIcebergCleanupJobStoreBackendTest extends TestJDBCBackend {

  private IcebergCleanupJobStore store;

  // TestJDBCBackend's BackendTestExtension overwrites GravitinoEnv's singleton "config" and
  // "idGenerator" fields with a backend-only Mockito mock and never restores them. Because the
  // whole iceberg-rest-server module runs in one JVM, that mock would leak into later test classes
  // (e.g. credential vending), where MetadataAuthzHelper.enableAuthorization() unboxes the
  // unstubbed config.get(ENABLE_AUTHORIZATION) -> null and NPEs. Snapshot the pre-test fields and
  // restore them after this class so it leaves GravitinoEnv exactly as it found it.
  private Object originalConfig;
  private Object originalIdGenerator;

  @BeforeAll
  public void snapshotGravitinoEnv() throws IllegalAccessException {
    originalConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    originalIdGenerator = FieldUtils.readField(GravitinoEnv.getInstance(), "idGenerator", true);
  }

  @AfterAll
  public void restoreGravitinoEnv() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", originalConfig, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "idGenerator", originalIdGenerator, true);
  }

  @BeforeEach
  public void prepareCleanupJobStore() {
    store = new IcebergCleanupJobStore(new RandomIdGenerator());
  }

  private static final long CATALOG_ID = 100L;
  private static final long OTHER_CATALOG_ID = 200L;

  private static IcebergCleanupJob sampleJob() {
    return new IcebergCleanupJob(
        0L,
        CATALOG_ID,
        "db",
        "t",
        "s3://b/db/t/metadata/0.json",
        "org.apache.iceberg.aws.s3.S3FileIO",
        ImmutableMap.of("k", "v"),
        "alice");
  }

  @TestTemplate
  void testAddTakeSucceedLifecycle() {
    Assertions.assertFalse(store.findUnfinishedJobId(CATALOG_ID, "db", "t").isPresent());

    long id = store.addJob(sampleJob());
    Assertions.assertTrue(id > 0);
    Assertions.assertTrue(store.findUnfinishedJobId(CATALOG_ID, "db", "t").isPresent());
    // The same namespace/table under a different catalog must not match.
    Assertions.assertFalse(store.findUnfinishedJobId(OTHER_CATALOG_ID, "db", "t").isPresent());

    long now = System.currentTimeMillis();
    Optional<IcebergCleanupJob> taken = store.takePendingJob(now, 300_000L, 10);
    Assertions.assertTrue(taken.isPresent());
    Assertions.assertEquals(id, taken.get().id());
    Assertions.assertEquals(ImmutableMap.of("k", "v"), taken.get().fileIOProperties());
    Assertions.assertEquals(IcebergCleanupJob.State.RUNNING, store.stateOf(id));
    Assertions.assertTrue(store.findUnfinishedJobId(CATALOG_ID, "db", "t").isPresent());
    Assertions.assertFalse(store.takePendingJob(now, 300_000L, 10).isPresent());

    Assertions.assertTrue(store.markSucceeded(id, now));
    Assertions.assertEquals(IcebergCleanupJob.State.SUCCEEDED, store.stateOf(id));
    // A second transition no longer owns the (now terminal) row, so it reports no update.
    Assertions.assertFalse(store.markSucceeded(id, now));
    Assertions.assertFalse(store.findUnfinishedJobId(CATALOG_ID, "db", "t").isPresent());
    Assertions.assertEquals(
        1, store.deleteFinishedJobsByLegacyTimeline(System.currentTimeMillis() + 1));
  }

  @TestTemplate
  void testTransientFailureRetriesThenFailsAtCeiling() {
    long id = store.addJob(sampleJob());
    for (int i = 0; i < 2; i++) {
      long heartbeat = System.currentTimeMillis();
      store.takePendingJob(heartbeat, 300_000L, 10);
      Assertions.assertTrue(store.recordFailure(id, "boom " + i, 3, heartbeat));
      Assertions.assertEquals(IcebergCleanupJob.State.PENDING, store.stateOf(id));
    }
    long heartbeat = System.currentTimeMillis();
    store.takePendingJob(heartbeat, 300_000L, 10);
    Assertions.assertTrue(store.recordFailure(id, "boom final", 3, heartbeat));
    Assertions.assertEquals(IcebergCleanupJob.State.FAILED, store.stateOf(id));
  }

  @TestTemplate
  void testRecordFailureAtMaxAttemptsMarksFailed() {
    long id = store.addJob(sampleJob());
    long heartbeat = System.currentTimeMillis();
    store.takePendingJob(heartbeat, 300_000L, 10);

    Assertions.assertTrue(store.recordFailure(id, "boom", 1, heartbeat));
    Assertions.assertEquals(IcebergCleanupJob.State.FAILED, store.stateOf(id));
  }

  @TestTemplate
  void testTerminalUpdateNeedsOwnership() {
    long id = store.addJob(sampleJob());
    long now = System.currentTimeMillis();
    store.takePendingJob(now, 300_000L, 10); // writes heartbeat_at = now

    // A stale heartbeat token (a reclaimed worker) cannot finish or fail the job.
    Assertions.assertFalse(store.markSucceeded(id, now - 1));
    Assertions.assertFalse(store.recordFailure(id, "stale", 3, now - 1));
    Assertions.assertEquals(IcebergCleanupJob.State.RUNNING, store.stateOf(id));

    // The owner with the current token wins.
    Assertions.assertTrue(store.markSucceeded(id, now));
    Assertions.assertEquals(IcebergCleanupJob.State.SUCCEEDED, store.stateOf(id));
  }

  @TestTemplate
  void testHeartbeatCasAndStaleTakeover() {
    long id = store.addJob(sampleJob());
    long t0 = System.currentTimeMillis();
    store.takePendingJob(t0, 300_000L, 10);
    Assertions.assertTrue(store.heartbeat(id, t0, t0 + 1000));
    Assertions.assertFalse(store.heartbeat(id, t0, t0 + 2000));
    // A stale RUNNING job can be taken again once its heartbeat ages past the timeout.
    Assertions.assertEquals(
        id, store.takePendingJob(t0 + 400_000L, 300_000L, 10).orElseThrow().id());
  }
}

class TestIcebergCleanupJobStoreBackend extends AbstractIcebergCleanupJobStoreBackendTest {}
