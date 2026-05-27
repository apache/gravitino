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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

  @TestTemplate
  void testDeleteAllBatchesEveryFile() {
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
  void testDeleteAllIgnoresAlreadyDeletedFiles() {
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
  void testDeleteAllIgnoresAlreadyDeletedBulkFiles() {
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
  void testCleanupFilesDeletesAllReachableFiles() throws Exception {
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("test", ImmutableMap.of());
    Namespace namespace = Namespace.of("db");
    catalog.createNamespace(namespace);
    TableIdentifier identifier = TableIdentifier.of(namespace, "t");
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    Table table = catalog.createTable(identifier, schema);

    // Append a data file so the snapshot has a manifest list, a manifest, and a data file path.
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("memory://db/t/data/00000-0.parquet")
            .withFileSizeInBytes(10L)
            .withRecordCount(1L)
            .build();
    table.newAppend().appendFile(dataFile).commit();

    BaseTable base = (BaseTable) catalog.loadTable(identifier);
    FileIO io = base.io();
    String metadataLocation = base.operations().current().metadataFileLocation();
    // Materialize the data file so its deletion is observable in the in-memory FileIO.
    ((InMemoryFileIO) io).addFile(dataFile.location(), new byte[] {1});

    // Capture every reachable file before cleanup; the manifest list/manifests cannot be read back
    // afterwards because they will have been deleted.
    List<String> expected = new ArrayList<>();
    expected.add(metadataLocation);
    expected.add(dataFile.location());
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
  void testWorkerRunsJobToSucceeded() {
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
  void testTransientFailureRetriesThenFails() {
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
          .atMost(10, TimeUnit.SECONDS)
          .until(() -> store.stateOf(id) == IcebergCleanupJob.State.FAILED);
    } finally {
      svc.close();
    }
  }

  @TestTemplate
  void testIsNameOccupiedDelegatesToStore() {
    IcebergCleanupManager svc =
        new IcebergCleanupManager(store, new IcebergConfig(new HashMap<>()));
    try {
      Assertions.assertFalse(svc.isNameOccupied(CATALOG_ID, "db", "t"));
      svc.enqueue(sampleJob());
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
}
