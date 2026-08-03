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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.types.Types;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Verifies that cleanup manifest progress is advisory, deduplicated, and ownership-fenced. */
class TestIcebergCleanupManifestProgress extends TestJDBCBackend {

  private static final long CATALOG_ID = 100L;

  private IcebergCleanupJobStore store;
  private Object originalConfig;
  private Object originalIdGenerator;

  @BeforeAll
  void snapshotGravitinoEnv() throws IllegalAccessException {
    originalConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    originalIdGenerator = FieldUtils.readField(GravitinoEnv.getInstance(), "idGenerator", true);
  }

  @AfterAll
  void restoreGravitinoEnv() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", originalConfig, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "idGenerator", originalIdGenerator, true);
  }

  @BeforeEach
  void prepareStore() {
    store = new IcebergCleanupJobStore(new RandomIdGenerator());
  }

  @AfterEach
  void clearFileIO() {
    DelegatingFileIO.clear();
  }

  @TestTemplate
  void testCompletedJobReportsEveryUniqueManifestOnce() {
    BaseTable table = tableWithSnapshots(3);
    FileIO io = table.io();
    Set<String> uniqueManifests = new LinkedHashSet<>();
    int rawManifestReferences = 0;
    for (Snapshot snapshot : table.snapshots()) {
      for (ManifestFile manifest : snapshot.allManifests(io)) {
        rawManifestReferences++;
        uniqueManifests.add(manifest.path());
      }
    }
    Assertions.assertTrue(rawManifestReferences > uniqueManifests.size());

    long id = runCleanupToCompletion(store, table, defaultConfig());
    IcebergCleanupJobStatus status = store.getStatus(id).orElseThrow();
    Assertions.assertEquals(IcebergCleanupJob.State.SUCCEEDED, status.state());
    Assertions.assertEquals((long) uniqueManifests.size(), status.manifestsTotal());
    Assertions.assertEquals((long) uniqueManifests.size(), status.manifestsDone());
  }

  @TestTemplate
  void testFailedProgressHeartbeatDoesNotFailCleanup() {
    FailingOnceHeartbeatStore failingStore = new FailingOnceHeartbeatStore();
    BaseTable table = tableWithSnapshots(1);

    long id = runCleanupToCompletion(failingStore, table, defaultConfig());
    IcebergCleanupJobStatus status = failingStore.getStatus(id).orElseThrow();
    Assertions.assertEquals(IcebergCleanupJob.State.SUCCEEDED, status.state());
    Assertions.assertNull(status.manifestsTotal());
    Assertions.assertNull(status.manifestsDone());
  }

  @TestTemplate
  void testLostProgressOwnershipReclaimsAndRestartsAtZero() {
    LoseOwnershipOnceStore losingStore = new LoseOwnershipOnceStore();
    BaseTable table = tableWithSnapshots(1);
    Map<String, String> properties = defaultConfig();
    properties.put("async-cleanup.heartbeat-timeout-secs", "1");

    long id = runCleanupToCompletion(losingStore, table, properties);
    IcebergCleanupJobStatus status = losingStore.getStatus(id).orElseThrow();
    Assertions.assertEquals(IcebergCleanupJob.State.SUCCEEDED, status.state());
    Assertions.assertEquals(0L, status.manifestsTotal());
    Assertions.assertEquals(0L, status.manifestsDone());
    Assertions.assertTrue(losingStore.progressHeartbeatCalls() >= 2);
  }

  @TestTemplate
  void testProgressHeartbeatRequiresCurrentOwnershipToken() {
    long id = store.addJob(sampleJob("memory://db/t/metadata/0.json"));
    long firstClaim = System.currentTimeMillis();
    store.takePendingJob(firstClaim, 300_000L, 10).orElseThrow();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> store.heartbeat(id, firstClaim, firstClaim + 1L, 2L, 3L));
    Assertions.assertTrue(store.heartbeat(id, firstClaim, firstClaim + 1L, 3L, 2L));
    Assertions.assertFalse(store.heartbeat(id, firstClaim, firstClaim + 2L, 99L, 99L));
    IcebergCleanupJobStatus beforeReclaim = store.getStatus(id).orElseThrow();
    Assertions.assertEquals(3L, beforeReclaim.manifestsTotal());
    Assertions.assertEquals(2L, beforeReclaim.manifestsDone());

    long secondClaim = firstClaim + 400_000L;
    store.takePendingJob(secondClaim, 300_000L, 10).orElseThrow();
    Assertions.assertTrue(store.heartbeat(id, secondClaim, secondClaim + 1L, 0L, 0L));
    IcebergCleanupJobStatus restarted = store.getStatus(id).orElseThrow();
    Assertions.assertEquals(0L, restarted.manifestsTotal());
    Assertions.assertEquals(0L, restarted.manifestsDone());

    Assertions.assertTrue(store.heartbeat(id, secondClaim + 1L, secondClaim + 2L, 3L, 3L));
    Assertions.assertTrue(store.markSucceeded(id, secondClaim + 2L));
    IcebergCleanupJobStatus completed = store.getStatus(id).orElseThrow();
    Assertions.assertEquals(IcebergCleanupJob.State.SUCCEEDED, completed.state());
    Assertions.assertEquals(3L, completed.manifestsTotal());
    Assertions.assertEquals(3L, completed.manifestsDone());
  }

  private static long runCleanupToCompletion(
      IcebergCleanupJobStore jobStore, BaseTable table, Map<String, String> properties) {
    DelegatingFileIO.use(table.io());
    String metadataLocation = table.operations().current().metadataFileLocation();
    long id = jobStore.addJob(sampleJob(metadataLocation));
    IcebergCleanupManager manager =
        new IcebergCleanupManager(jobStore, new IcebergConfig(properties));
    manager.start();
    try {
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .until(() -> jobStore.stateOf(id) == IcebergCleanupJob.State.SUCCEEDED);
      return id;
    } finally {
      manager.close();
    }
  }

  private static Map<String, String> defaultConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("async-cleanup.worker-threads", "1");
    properties.put("async-cleanup.poll-interval-secs", "1");
    return properties;
  }

  private static IcebergCleanupJob sampleJob(String metadataLocation) {
    return new IcebergCleanupJob(
        0L,
        CATALOG_ID,
        "db",
        "t",
        metadataLocation,
        DelegatingFileIO.class.getName(),
        ImmutableMap.of(),
        "tester");
  }

  private static BaseTable tableWithSnapshots(int snapshots) {
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("test", ImmutableMap.of());
    catalog.createNamespace(Namespace.of("db"));
    TableIdentifier id = TableIdentifier.of(Namespace.of("db"), "t");
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    Table table = catalog.createTable(id, schema);
    table.updateProperties().set(TableProperties.MANIFEST_MERGE_ENABLED, "false").commit();
    for (int index = 0; index < snapshots; index++) {
      String path = "memory://db/t/data/" + index + ".parquet";
      DataFile dataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath(path)
              .withFileSizeInBytes(10L)
              .withRecordCount(1L)
              .build();
      table.newAppend().appendFile(dataFile).commit();
      ((InMemoryFileIO) table.io()).addFile(path, new byte[] {1});
    }
    return (BaseTable) catalog.loadTable(id);
  }

  /** FileIO that lets a worker-loaded instance access an in-memory table created by the test. */
  public static class DelegatingFileIO implements SupportsBulkOperations {

    private static volatile FileIO delegate;

    static void use(FileIO fileIO) {
      delegate = fileIO;
    }

    static void clear() {
      delegate = null;
    }

    @Override
    public void initialize(Map<String, String> properties) {}

    @Override
    public InputFile newInputFile(String path) {
      return delegate.newInputFile(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      delegate.deleteFile(path);
    }

    @Override
    public void deleteFiles(Iterable<String> paths) {
      for (String path : paths) {
        delegate.deleteFile(path);
      }
    }

    @Override
    public void close() {}
  }

  private static class FailingOnceHeartbeatStore extends IcebergCleanupJobStore {

    private final AtomicBoolean fail = new AtomicBoolean(true);

    private FailingOnceHeartbeatStore() {
      super(new RandomIdGenerator());
    }

    @Override
    public boolean heartbeat(
        long id, long lastHeartbeat, long now, long manifestsTotal, long manifestsDone) {
      if (fail.compareAndSet(true, false)) {
        throw new IllegalStateException("test progress write failure");
      }
      return super.heartbeat(id, lastHeartbeat, now, manifestsTotal, manifestsDone);
    }
  }

  private static class LoseOwnershipOnceStore extends IcebergCleanupJobStore {

    private final AtomicBoolean lose = new AtomicBoolean(true);
    private int progressHeartbeatCalls;

    private LoseOwnershipOnceStore() {
      super(new RandomIdGenerator());
    }

    @Override
    public synchronized boolean heartbeat(
        long id, long lastHeartbeat, long now, long manifestsTotal, long manifestsDone) {
      progressHeartbeatCalls++;
      if (lose.compareAndSet(true, false)) {
        return false;
      }
      return super.heartbeat(id, lastHeartbeat, now, manifestsTotal, manifestsDone);
    }

    private synchronized int progressHeartbeatCalls() {
      return progressHeartbeatCalls;
    }
  }
}
