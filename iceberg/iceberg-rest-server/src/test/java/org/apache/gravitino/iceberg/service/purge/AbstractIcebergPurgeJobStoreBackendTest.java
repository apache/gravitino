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

package org.apache.gravitino.iceberg.service.purge;

import com.google.common.collect.ImmutableMap;
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
 * Shared purge-store test logic exercised against the same relational backend matrix as core
 * metadata service tests. {@link TestJDBCBackend} initializes H2 by default, adds MySQL and
 * PostgreSQL when {@code dockerTest=true}, and truncates all backend tables before each invocation.
 */
abstract class AbstractIcebergPurgeJobStoreBackendTest extends TestJDBCBackend {

  private IcebergPurgeJobStore store;

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
  public void preparePurgeJobStore() {
    store = new IcebergPurgeJobStore(new RandomIdGenerator());
  }

  private static IcebergPurgeJob sampleJob() {
    return new IcebergPurgeJob(
        0L,
        "ml",
        "cat",
        "db",
        "t",
        "s3://b/db/t/metadata/0.json",
        "org.apache.iceberg.aws.s3.S3FileIO",
        ImmutableMap.of("k", "v"),
        "alice");
  }

  @TestTemplate
  void testAddTakeSucceedLifecycle() {
    Assertions.assertFalse(store.hasActiveJob("cat", "db", "t"));

    long id = store.addJob(sampleJob());
    Assertions.assertTrue(id > 0);
    Assertions.assertTrue(store.hasActiveJob("cat", "db", "t"));

    long now = System.currentTimeMillis();
    IcebergPurgeJob claimed = store.takePendingJob(now, 300_000L, 10);
    Assertions.assertNotNull(claimed);
    Assertions.assertEquals(id, claimed.id());
    Assertions.assertEquals(ImmutableMap.of("k", "v"), claimed.fileIOProperties());
    Assertions.assertEquals(IcebergPurgeJob.State.RUNNING, store.stateOf(id));
    Assertions.assertTrue(store.hasActiveJob("cat", "db", "t"));
    Assertions.assertNull(store.takePendingJob(now, 300_000L, 10));

    store.markSucceeded(id);
    Assertions.assertEquals(IcebergPurgeJob.State.SUCCEEDED, store.stateOf(id));
    Assertions.assertFalse(store.hasActiveJob("cat", "db", "t"));
    Assertions.assertEquals(1, store.pruneFinishedBefore(System.currentTimeMillis() + 1));
  }

  @TestTemplate
  void testMarkFailedTerminal() {
    long id = store.addJob(sampleJob());
    store.takePendingJob(System.currentTimeMillis(), 300_000L, 10);
    store.markFailed(id, "corrupt metadata");
    Assertions.assertEquals(IcebergPurgeJob.State.FAILED, store.stateOf(id));
  }

  @TestTemplate
  void testTransientFailureRetriesThenFailsAtCeiling() {
    long id = store.addJob(sampleJob());
    for (int i = 0; i < 2; i++) {
      store.takePendingJob(System.currentTimeMillis(), 300_000L, 10);
      store.recordFailure(id, "boom " + i, 3);
      Assertions.assertEquals(IcebergPurgeJob.State.PENDING, store.stateOf(id));
    }
    store.takePendingJob(System.currentTimeMillis(), 300_000L, 10);
    store.recordFailure(id, "boom final", 3);
    Assertions.assertEquals(IcebergPurgeJob.State.FAILED, store.stateOf(id));
  }

  @TestTemplate
  void testHeartbeatCasAndStaleReclaim() {
    long id = store.addJob(sampleJob());
    long t0 = System.currentTimeMillis();
    store.takePendingJob(t0, 300_000L, 10);
    Assertions.assertTrue(store.heartbeat(id, t0, t0 + 1000));
    Assertions.assertFalse(store.heartbeat(id, t0, t0 + 2000));
    // A stale RUNNING job is reclaimable once its heartbeat ages past the timeout.
    Assertions.assertEquals(id, store.takePendingJob(t0 + 400_000L, 300_000L, 10).id());
  }
}

class TestIcebergPurgeJobStoreBackend extends AbstractIcebergPurgeJobStoreBackendTest {}
