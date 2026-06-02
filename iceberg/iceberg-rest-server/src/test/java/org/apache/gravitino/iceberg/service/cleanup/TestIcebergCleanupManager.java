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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.types.Types;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * Exercises {@link IcebergCleanupManager} against the same relational backend matrix as the cleanup
 * store: {@link TestJDBCBackend} initializes H2 by default, adds MySQL and PostgreSQL when {@code
 * dockerTest=true}, and truncates all backend tables before each invocation.
 */
class TestIcebergCleanupManager extends TestJDBCBackend {

  private static final long CATALOG_ID = 100L;
  private static final String DATA_FILE = "memory://db/t/data/00000-0.parquet";

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

  private static IcebergConfig fastPollConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("async-cleanup.worker-threads", "1");
    config.put("async-cleanup.poll-interval-secs", "1");
    return new IcebergConfig(config);
  }

  private static IcebergCleanupJob sampleJob() {
    return new IcebergCleanupJob(
        0L,
        CATALOG_ID,
        "db",
        "t",
        "s3://b/db/t/metadata/0.json",
        NoopFileIO.class.getName(),
        ImmutableMap.of(),
        "alice");
  }

  // Builds db.t with one appended data file (so it has a manifest list, a manifest, and a data
  // file) and materializes the data file in the in-memory FileIO so deletions are observable.
  private static BaseTable tableWithDataFile() {
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("test", ImmutableMap.of());
    catalog.createNamespace(Namespace.of("db"));
    TableIdentifier id = TableIdentifier.of(Namespace.of("db"), "t");
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    Table table = catalog.createTable(id, schema);
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(DATA_FILE)
            .withFileSizeInBytes(10L)
            .withRecordCount(1L)
            .build();
    table.newAppend().appendFile(dataFile).commit();
    BaseTable base = (BaseTable) catalog.loadTable(id);
    ((InMemoryFileIO) base.io()).addFile(DATA_FILE, new byte[] {1});
    return base;
  }

  @TestTemplate
  void testDeleteAllBatches() {
    CopyOnWriteArrayList<String> deleted = new CopyOnWriteArrayList<>();
    IcebergCleanupManager svc =
        new IcebergCleanupManager(store, new IcebergConfig(new HashMap<>()));
    try {
      svc.deleteAll(new RecordingFileIO(deleted), Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
      Assertions.assertEquals(7, deleted.size());
    } finally {
      svc.close();
    }
  }

  @TestTemplate
  void testDeleteAllIgnoresMissing() {
    IcebergCleanupManager svc =
        new IcebergCleanupManager(store, new IcebergConfig(new HashMap<>()));
    try {
      Assertions.assertDoesNotThrow(
          () -> svc.deleteAll(new MissingFileIO(), Arrays.asList("already-gone")));
    } finally {
      svc.close();
    }
  }

  @TestTemplate
  void testDeleteAllIgnoresMissingBulk() {
    IcebergCleanupManager svc =
        new IcebergCleanupManager(store, new IcebergConfig(new HashMap<>()));
    try {
      Assertions.assertDoesNotThrow(
          () -> svc.deleteAll(new MissingBulkFileIO(), Arrays.asList("already-gone")));
    } finally {
      svc.close();
    }
  }

  @TestTemplate
  void testCleanupDeletesAllFiles() {
    BaseTable base = tableWithDataFile();
    FileIO io = base.io();
    String metadataLocation = base.operations().current().metadataFileLocation();

    // Capture reachable files before cleanup; the manifests cannot be read once deleted.
    List<String> expected = new ArrayList<>();
    expected.add(metadataLocation);
    expected.add(DATA_FILE);
    for (Snapshot snapshot : base.snapshots()) {
      expected.add(snapshot.manifestListLocation());
      for (ManifestFile manifest : snapshot.allManifests(io)) {
        expected.add(manifest.path());
      }
    }
    expected.forEach(file -> Assertions.assertTrue(((InMemoryFileIO) io).fileExists(file), file));

    IcebergCleanupManager svc =
        new IcebergCleanupManager(store, new IcebergConfig(new HashMap<>()));
    try {
      svc.cleanupFiles(io, metadataLocation);
    } finally {
      svc.close();
    }

    expected.forEach(file -> Assertions.assertFalse(((InMemoryFileIO) io).fileExists(file), file));
  }

  @TestTemplate
  void testCleanupToleratesMissingMetadata() {
    // A missing root metadata.json means the table is already gone, so cleanup just returns.
    IcebergCleanupManager svc =
        new IcebergCleanupManager(store, new IcebergConfig(new HashMap<>()));
    try {
      Assertions.assertDoesNotThrow(
          () -> svc.cleanupFiles(new InMemoryFileIO(), "memory://db/t/metadata/missing.json"));
    } finally {
      svc.close();
    }
  }

  @TestTemplate
  void testWorkerSucceeds() {
    AtomicInteger calls = new AtomicInteger();
    IcebergCleanupManager svc =
        new IcebergCleanupManager(store, fastPollConfig()) {
          @Override
          void cleanupFiles(FileIO io, String metadataLocation) {
            calls.incrementAndGet();
          }
        };
    long id = store.addJob(sampleJob());
    svc.start();
    try {
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .until(() -> store.stateOf(id) == IcebergCleanupJob.State.SUCCEEDED);
      Assertions.assertEquals(1, calls.get());
    } finally {
      svc.close();
    }
  }

  @TestTemplate
  void testWorkerRetriesThenFails() {
    Map<String, String> config = new HashMap<>();
    config.put("async-cleanup.worker-threads", "1");
    config.put("async-cleanup.poll-interval-secs", "1");
    config.put("async-cleanup.max-attempts", "3");
    IcebergCleanupManager svc =
        new IcebergCleanupManager(store, new IcebergConfig(config)) {
          @Override
          void cleanupFiles(FileIO io, String metadataLocation) {
            throw new RuntimeException("transient");
          }
        };
    long id = store.addJob(sampleJob());
    svc.start();
    try {
      Awaitility.await()
          .atMost(15, TimeUnit.SECONDS)
          .until(() -> store.stateOf(id) == IcebergCleanupJob.State.FAILED);
    } finally {
      svc.close();
    }
  }

  @TestTemplate
  void testStartAfterCloseFailsFast() {
    IcebergCleanupManager svc =
        new IcebergCleanupManager(store, new IcebergConfig(new HashMap<>()));
    svc.close();

    Assertions.assertThrows(IllegalStateException.class, svc::start);
  }

  @TestTemplate
  @SuppressWarnings("unchecked")
  void testRefreshHeartbeatsPublishesNewTokenBeforeStoreUpdate() throws Exception {
    BlockingHeartbeatStore blockingStore = new BlockingHeartbeatStore();
    IcebergCleanupManager svc =
        new IcebergCleanupManager(blockingStore, new IcebergConfig(new HashMap<>()));
    try {
      Map<Long, Long> heartbeats =
          (Map<Long, Long>) FieldUtils.readField(svc, "ownedHeartbeats", true);
      heartbeats.put(1L, 100L);

      Thread refresh = new Thread(svc::refreshHeartbeats);
      refresh.start();
      Assertions.assertTrue(blockingStore.heartbeatStarted.await(5, TimeUnit.SECONDS));

      AtomicLong token = new AtomicLong();
      svc.finishJob(1L, heartbeat -> token.compareAndSet(0L, heartbeat));
      Assertions.assertTrue(token.get() > 100L);

      blockingStore.releaseHeartbeat.countDown();
      refresh.join(5_000L);
      Assertions.assertFalse(refresh.isAlive());
    } finally {
      svc.close();
    }
  }

  @TestTemplate
  void testIsNameOccupied() {
    IcebergCleanupManager svc =
        new IcebergCleanupManager(store, new IcebergConfig(new HashMap<>()));
    try {
      Assertions.assertFalse(svc.isNameOccupied(CATALOG_ID, "db", "t"));
      svc.addJob(sampleJob());
      Assertions.assertTrue(svc.isNameOccupied(CATALOG_ID, "db", "t"));
    } finally {
      svc.close();
    }
  }

  static class RecordingFileIO implements SupportsBulkOperations {
    private final CopyOnWriteArrayList<String> deleted;

    RecordingFileIO(CopyOnWriteArrayList<String> deleted) {
      this.deleted = deleted;
    }

    @Override
    public InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      deleted.add(path);
    }

    @Override
    public void deleteFiles(Iterable<String> paths) {
      for (String path : paths) {
        deleted.add(path);
      }
    }
  }

  private static class MissingBulkFileIO extends MissingFileIO implements SupportsBulkOperations {
    @Override
    public void deleteFiles(Iterable<String> paths) {
      throw new NotFoundException("Missing files");
    }
  }

  public static class NoopFileIO implements FileIO {
    @Override
    public InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException();
    }
  }

  private static class MissingFileIO implements FileIO {
    @Override
    public InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      throw new NotFoundException("Missing file: %s", path);
    }
  }

  private static class BlockingHeartbeatStore extends IcebergCleanupJobStore {
    private final CountDownLatch heartbeatStarted = new CountDownLatch(1);
    private final CountDownLatch releaseHeartbeat = new CountDownLatch(1);

    BlockingHeartbeatStore() {
      super(new RandomIdGenerator());
    }

    @Override
    public boolean heartbeat(long id, long lastHeartbeat, long now) {
      heartbeatStarted.countDown();
      try {
        Assertions.assertTrue(releaseHeartbeat.await(5, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      return true;
    }
  }
}
