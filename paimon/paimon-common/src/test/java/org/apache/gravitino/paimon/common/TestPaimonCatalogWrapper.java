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
package org.apache.gravitino.paimon.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Regression tests for backend selection, table identity and locked snapshot creation. */
public class TestPaimonCatalogWrapper {

  @TempDir Path directory;

  @Test
  void testTableTypeFilteringIncludesDefaultAndExactTypes() throws Exception {
    Catalog catalog = mock(Catalog.class);
    when(catalog.listTables("db")).thenReturn(List.of("default", "explicit", "format"));
    Table ordinary = mock(Table.class);
    Table explicit = mock(Table.class);
    Table format = mock(Table.class);
    when(catalog.getTable(Identifier.create("db", "default"))).thenReturn(ordinary);
    when(catalog.getTable(Identifier.create("db", "explicit"))).thenReturn(explicit);
    when(catalog.getTable(Identifier.create("db", "format"))).thenReturn(format);
    when(ordinary.options()).thenReturn(Map.of());
    when(explicit.options()).thenReturn(Map.of("type", "table"));
    when(format.options()).thenReturn(Map.of("type", "format-table"));
    try (PaimonCatalogWrapper wrapper = new PaimonCatalogWrapper("test", new Options(), catalog)) {
      assertEquals(List.of("default", "explicit"), wrapper.listTables("db", "table"));
      assertEquals(List.of("format"), wrapper.listTables("db", "format-table"));
      assertEquals(List.of("default", "explicit", "format"), wrapper.listTables("db", ""));
    }
  }

  @Test
  void testTableTypeFilteringSkipsConcurrentlyDroppedTables() throws Exception {
    Catalog catalog = mock(Catalog.class);
    when(catalog.listTables("db")).thenReturn(List.of("dropped", "remaining"));
    when(catalog.getTable(Identifier.create("db", "dropped")))
        .thenThrow(new Catalog.TableNotExistException(Identifier.create("db", "dropped")));
    Table remaining = mock(Table.class);
    when(remaining.options()).thenReturn(Map.of());
    when(catalog.getTable(Identifier.create("db", "remaining"))).thenReturn(remaining);
    when(catalog.listTables("missing")).thenThrow(new Catalog.DatabaseNotExistException("missing"));
    try (PaimonCatalogWrapper wrapper = new PaimonCatalogWrapper("test", new Options(), catalog)) {
      assertEquals(List.of("remaining"), wrapper.listTables("db", "table"));
      PaimonCatalogException error =
          assertThrows(PaimonCatalogException.class, () -> wrapper.listTables("missing", "table"));
      assertEquals(PaimonCatalogException.Reason.NOT_FOUND, error.reason());
      assertEquals(PaimonCatalogException.Resource.DATABASE, error.resource());
    }
  }

  @Test
  void testConcurrentExternalIdentityInitializationAcrossBranches() throws Exception {
    Catalog catalog = mock(Catalog.class);
    FileStoreTable table = mock(FileStoreTable.class);
    FileIO fileIO = mock(FileIO.class);
    CatalogEnvironment environment = mock(CatalogEnvironment.class);
    CatalogLockFactory factory = mock(CatalogLockFactory.class);
    CatalogLock backendLock = mock(CatalogLock.class);
    Identifier main = Identifier.create("db", "t");
    Identifier branch = new Identifier("db", "t", "b1");
    org.apache.paimon.fs.Path location = new org.apache.paimon.fs.Path("s3://bucket/t");
    org.apache.paimon.fs.Path uuidPath = new org.apache.paimon.fs.Path(location, ".rest-uuid");
    when(catalog.getTable(main)).thenReturn(table);
    when(catalog.getTable(branch)).thenReturn(table);
    when(table.location()).thenReturn(location);
    when(table.options()).thenReturn(Map.of());
    when(table.fileIO()).thenReturn(fileIO);
    when(table.catalogEnvironment()).thenReturn(environment);
    when(environment.lockFactory()).thenReturn(factory);
    when(factory.createLock(any())).thenReturn(backendLock);
    Map<String, ReentrantLock> locks = new ConcurrentHashMap<>();
    when(backendLock.runWithLock(eq("db"), anyString(), any()))
        .thenAnswer(
            invocation -> {
              ReentrantLock lock =
                  locks.computeIfAbsent(invocation.getArgument(1), key -> new ReentrantLock());
              lock.lock();
              try {
                Callable<?> action = invocation.getArgument(2);
                return action.call();
              } finally {
                lock.unlock();
              }
            });
    CyclicBarrier initialReads = new CyclicBarrier(2);
    AtomicInteger reads = new AtomicInteger();
    AtomicReference<String> stored = new AtomicReference<>();
    when(fileIO.exists(uuidPath))
        .thenAnswer(
            invocation -> {
              if (reads.incrementAndGet() <= 2) {
                initialReads.await(10, TimeUnit.SECONDS);
                return false;
              }
              return stored.get() != null;
            });
    when(fileIO.tryToWriteAtomic(eq(uuidPath), anyString()))
        .thenAnswer(
            invocation -> {
              assertTrue(locks.get("t").isHeldByCurrentThread());
              // Model an object store whose rename overwrites an existing destination.
              stored.set(invocation.getArgument(1));
              return true;
            });
    when(fileIO.readFileUtf8(uuidPath)).thenAnswer(invocation -> stored.get());
    try (PaimonCatalogWrapper first = new PaimonCatalogWrapper("test", new Options(), catalog);
        PaimonCatalogWrapper second = new PaimonCatalogWrapper("test", new Options(), catalog)) {
      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        Future<String> mainId = executor.submit(() -> first.getTable(main).id());
        Future<String> branchId = executor.submit(() -> second.getTable(branch).id());
        String id = mainId.get(20, TimeUnit.SECONDS);
        assertNotNull(id);
        assertEquals(id, branchId.get(20, TimeUnit.SECONDS));
        assertEquals(id, second.getTable(main).id());
        assertEquals(id, first.getTable(branch).id());
        verify(fileIO, times(1)).tryToWriteAtomic(eq(uuidPath), anyString());
        verify(backendLock, times(2)).runWithLock(eq("db"), eq("t"), any());
        verify(backendLock, times(2)).close();
      } finally {
        executor.shutdownNow();
      }
    }
  }

  @Test
  void testBackendLockIsClosedAfterFailedCommit() throws Exception {
    Catalog catalog = mock(Catalog.class);
    FileStoreTable table = mock(FileStoreTable.class);
    CatalogEnvironment environment = mock(CatalogEnvironment.class);
    CatalogLockFactory factory = mock(CatalogLockFactory.class);
    CatalogLock lock = mock(CatalogLock.class);
    Identifier identifier = Identifier.create("db", "t");
    when(catalog.getTable(identifier)).thenReturn(table);
    when(table.catalogEnvironment()).thenReturn(environment);
    when(environment.lockFactory()).thenReturn(factory);
    when(factory.createLock(any())).thenReturn(lock);
    when(lock.runWithLock(eq("db"), eq("t"), any())).thenThrow(new IOException("failed 100%"));
    try (PaimonCatalogWrapper wrapper = new PaimonCatalogWrapper("test", new Options(), catalog)) {
      PaimonCatalogException error =
          assertThrows(
              PaimonCatalogException.class,
              () -> wrapper.commitTable(identifier, "table-id", null, snapshot("writer")));
      assertTrue(error.getMessage().contains("failed 100%"));
      verify(lock).close();
    }
  }

  @Test
  void testJdbcBackendAndCredentialsAreUsed() throws Exception {
    Options options =
        Options.fromMap(
            Map.of(
                "catalog-backend", "jdbc",
                "jdbc-driver", "org.sqlite.JDBC",
                "jdbc-user", "test-user",
                "jdbc-password", "test-password",
                "uri", "jdbc:sqlite:" + directory.resolve("catalog.db"),
                "warehouse", directory.resolve("warehouse").toUri().toString()));
    try (PaimonCatalogWrapper wrapper = new PaimonCatalogWrapper("jdbc", options)) {
      wrapper.createDatabase("db", Map.of());
      wrapper.createTable(Identifier.create("db", "t"), schema());
      assertEquals("jdbc", wrapper.catalogOptions().get("metastore"));
      assertEquals("test-user", wrapper.catalogOptions().get("jdbc.user"));
      assertEquals("test-password", wrapper.catalogOptions().get("jdbc.password"));
      try (Catalog nativeCatalog =
          CatalogFactory.createCatalog(CatalogContext.create(wrapper.catalogOptions()))) {
        assertTrue(nativeCatalog.listDatabases().contains("db"));
        assertNotNull(nativeCatalog.getTable(Identifier.create("db", "t")));
      }
    }
  }

  @Test
  void testInvalidJdbcUriDoesNotFallBackToFilesystem() {
    assertThrows(
        RuntimeException.class,
        () ->
            new PaimonCatalogWrapper(
                "jdbc",
                Options.fromMap(
                    Map.of(
                        "catalog-backend", "jdbc",
                        "uri", "jdbc:nonexistent:catalog",
                        "warehouse", directory.toUri().toString()))));
  }

  @Test
  void testRestTableIdentitySurvivesRestart() throws Exception {
    Options options = options();
    Identifier identifier = Identifier.create("db", "t");
    String id;
    try (PaimonCatalogWrapper wrapper = new PaimonCatalogWrapper("test", options)) {
      wrapper.createDatabase("db", Map.of());
      wrapper.createTable(identifier, schema());
      id = wrapper.getTable(identifier).id();
    }
    try (PaimonCatalogWrapper restarted = new PaimonCatalogWrapper("test", options)) {
      assertEquals(id, restarted.getTable(identifier).id());
    }
  }

  @Test
  void testExternalIdentityPersistsAndRejectsStaleCommitAfterRecreation() throws Exception {
    Options options = options();
    Identifier identifier = Identifier.create("db", "t");
    try (Catalog external = CatalogFactory.createCatalog(CatalogContext.create(options));
        PaimonCatalogWrapper wrapper = new PaimonCatalogWrapper("test", options)) {
      external.createDatabase("db", false);
      external.createTable(identifier, schema(), false);
      String oldId = wrapper.getTable(identifier).id();
      Identifier renamed = Identifier.create("db", "renamed");
      external.renameTable(identifier, renamed, false);
      assertEquals(oldId, wrapper.getTable(renamed).id());
      external.renameTable(renamed, identifier, false);
      try (PaimonCatalogWrapper restarted = new PaimonCatalogWrapper("test", options)) {
        assertEquals(oldId, restarted.getTable(identifier).id());
      }
      external.dropTable(identifier, false);
      external.createTable(identifier, schema(), false);
      assertStaleTableId(wrapper, identifier, oldId);
      String newId = wrapper.getTable(identifier).id();
      assertNotEquals(oldId, newId);
      assertStaleTableId(wrapper, identifier, oldId);
      assertFalse(Files.exists(directory.resolve("db.db/t/snapshot/snapshot-1")));
      assertTrue(wrapper.commitTable(identifier, newId, null, snapshot("new-writer")));
    }
  }

  @Test
  void testConcurrentExternalIdentityInitialization() throws Exception {
    Options options = options();
    Identifier identifier = Identifier.create("db", "t");
    try (Catalog external = CatalogFactory.createCatalog(CatalogContext.create(options));
        PaimonCatalogWrapper first = new PaimonCatalogWrapper("test", options);
        PaimonCatalogWrapper second = new PaimonCatalogWrapper("test", options)) {
      external.createDatabase("db", false);
      external.createTable(identifier, schema(), false);
      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        CyclicBarrier start = new CyclicBarrier(2);
        Future<String> firstId =
            executor.submit(
                () -> {
                  start.await(10, TimeUnit.SECONDS);
                  return first.getTable(identifier).id();
                });
        Future<String> secondId =
            executor.submit(
                () -> {
                  start.await(10, TimeUnit.SECONDS);
                  return second.getTable(identifier).id();
                });
        assertEquals(firstId.get(20, TimeUnit.SECONDS), secondId.get(20, TimeUnit.SECONDS));
      } finally {
        executor.shutdownNow();
      }
    }
  }

  @Test
  void testConcurrentFilesystemCommitsWithoutBackendLock() throws Exception {
    Options options = options();
    Identifier identifier = Identifier.create("db", "t");
    try (PaimonCatalogWrapper first = new PaimonCatalogWrapper("test", options);
        PaimonCatalogWrapper second = new PaimonCatalogWrapper("test", options)) {
      first.createDatabase("db", Map.of());
      first.createTable(identifier, schema());
      String tableId = first.getTable(identifier).id();
      assertNull(first.getFileTable(identifier).catalogEnvironment().lockFactory());
      ExecutorService executor = Executors.newFixedThreadPool(2);
      CyclicBarrier start = new CyclicBarrier(2);
      try {
        Future<Boolean> left =
            executor.submit(
                () -> {
                  start.await(10, TimeUnit.SECONDS);
                  return first.commitTable(identifier, tableId, null, snapshot("left"));
                });
        Future<Boolean> right =
            executor.submit(
                () -> {
                  start.await(10, TimeUnit.SECONDS);
                  return second.commitTable(identifier, tableId, null, snapshot("right"));
                });
        boolean leftCommitted = left.get(20, TimeUnit.SECONDS);
        assertNotEquals(leftCommitted, right.get(20, TimeUnit.SECONDS));
        assertEquals(
            leftCommitted ? "left" : "right",
            first.loadLatestSnapshot(identifier).orElseThrow().commitUser());
      } finally {
        executor.shutdownNow();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"main", "b1"})
  void testConcurrentRestAndNativeCommitsShareBackendLock(String branch) throws Exception {
    Catalog catalog = mock(Catalog.class);
    FileStoreTable table = mock(FileStoreTable.class);
    CatalogEnvironment environment = mock(CatalogEnvironment.class);
    CatalogLockFactory factory = mock(CatalogLockFactory.class);
    CatalogLock backendLock = mock(CatalogLock.class);
    SnapshotManager snapshots = mock(SnapshotManager.class);
    FileIO fileIO = mock(FileIO.class);
    Identifier identifier = new Identifier("db", "t", branch);
    org.apache.paimon.fs.Path snapshotPath =
        new org.apache.paimon.fs.Path("s3://bucket/t/branch/snapshot-1");
    when(catalog.getTable(identifier)).thenReturn(table);
    when(table.options()).thenReturn(Map.of(PaimonCatalogWrapper.TABLE_UUID_OPTION, "table-id"));
    when(table.catalogEnvironment()).thenReturn(environment);
    when(environment.lockFactory()).thenReturn(factory);
    when(factory.createLock(any())).thenReturn(backendLock);
    when(table.snapshotManager()).thenReturn(snapshots);
    when(table.fileIO()).thenReturn(fileIO);
    when(snapshots.snapshotPath(1L)).thenReturn(snapshotPath);
    ReentrantLock storageLock = new ReentrantLock();
    when(backendLock.runWithLock(eq("db"), eq(identifier.getObjectName()), any()))
        .thenAnswer(
            invocation -> {
              storageLock.lock();
              try {
                Callable<?> action = invocation.getArgument(2);
                return action.call();
              } finally {
                storageLock.unlock();
              }
            });
    AtomicReference<String> stored = new AtomicReference<>();
    when(fileIO.exists(snapshotPath))
        .thenAnswer(
            invocation -> {
              assertTrue(storageLock.isHeldByCurrentThread());
              return stored.get() != null;
            });
    when(fileIO.tryToWriteAtomic(eq(snapshotPath), anyString()))
        .thenAnswer(
            invocation -> {
              assertTrue(storageLock.isHeldByCurrentThread());
              // Model object storage that overwrites on rename unless protected by an external
              // lock.
              stored.set(invocation.getArgument(1));
              return true;
            });
    try (PaimonCatalogWrapper wrapper = new PaimonCatalogWrapper("test", new Options(), catalog)) {
      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        CyclicBarrier start = new CyclicBarrier(2);
        Callable<Boolean> restWriter =
            () -> {
              start.await(10, TimeUnit.SECONDS);
              return wrapper.commitTable(identifier, "table-id", null, snapshot("writer"));
            };
        Callable<Boolean> nativeWriter =
            () -> {
              start.await(10, TimeUnit.SECONDS);
              try (Lock nativeLock = Lock.fromCatalog(backendLock, identifier)) {
                return nativeLock.runWithLock(
                    () -> {
                      if (fileIO.exists(snapshotPath)) {
                        return false;
                      }
                      return fileIO.tryToWriteAtomic(snapshotPath, "native-snapshot");
                    });
              }
            };
        Future<Boolean> first = executor.submit(restWriter);
        Future<Boolean> second = executor.submit(nativeWriter);
        assertNotEquals(first.get(20, TimeUnit.SECONDS), second.get(20, TimeUnit.SECONDS));
        verify(fileIO, times(1)).tryToWriteAtomic(eq(snapshotPath), anyString());
        verify(backendLock, times(2)).runWithLock(eq("db"), eq(identifier.getObjectName()), any());
        verify(backendLock, times(2)).close();
      } finally {
        executor.shutdownNow();
      }
    }
  }

  private Options options() {
    return Options.fromMap(Map.of("warehouse", directory.toUri().toString()));
  }

  private static void assertStaleTableId(
      PaimonCatalogWrapper wrapper, Identifier identifier, String tableId) {
    PaimonCatalogException failure =
        assertThrows(
            PaimonCatalogException.class,
            () -> wrapper.commitTable(identifier, tableId, null, snapshot("old-writer")));
    assertEquals(PaimonCatalogException.Reason.NOT_FOUND, failure.reason());
    assertEquals(PaimonCatalogException.Resource.TABLE, failure.resource());
  }

  @Test
  void testCheckpointAndRollbackShareFilesystemTableLock() throws Exception {
    Catalog catalog = mock(Catalog.class);
    FileStoreTable table = mock(FileStoreTable.class);
    SnapshotManager snapshots = mock(SnapshotManager.class);
    FileIO fileIO = mock(FileIO.class);
    Identifier identifier = Identifier.create("db", "t");
    org.apache.paimon.fs.Path location =
        new org.apache.paimon.fs.Path(directory.resolve("table").toUri().toString());
    org.apache.paimon.fs.Path snapshotPath = new org.apache.paimon.fs.Path(location, "snapshot");
    when(catalog.getTable(identifier)).thenReturn(table);
    when(table.location()).thenReturn(location);
    when(table.catalogEnvironment()).thenReturn(mock(CatalogEnvironment.class));
    when(table.options()).thenReturn(Map.of(PaimonCatalogWrapper.TABLE_UUID_OPTION, "table-id"));
    when(table.snapshotManager()).thenReturn(snapshots);
    when(table.fileIO()).thenReturn(fileIO);
    when(snapshots.snapshotPath(1L)).thenReturn(snapshotPath);
    when(snapshots.tryGetSnapshot(1L)).thenReturn(snapshot("writer"));

    CountDownLatch writerReady = new CountDownLatch(1);
    CountDownLatch rollbackStarted = new CountDownLatch(1);
    CountDownLatch preconditionChecked = new CountDownLatch(1);
    AtomicReference<Long> latest = new AtomicReference<>();
    when(fileIO.tryToWriteAtomic(eq(snapshotPath), anyString()))
        .thenAnswer(
            invocation -> {
              writerReady.countDown();
              assertTrue(rollbackStarted.await(5, TimeUnit.SECONDS));
              assertFalse(preconditionChecked.await(200, TimeUnit.MILLISECONDS));
              return true;
            });
    doAnswer(
            invocation -> {
              latest.set(1L);
              return null;
            })
        .when(snapshots)
        .commitLatestHint(1L);
    when(snapshots.latestSnapshotId())
        .thenAnswer(
            invocation -> {
              preconditionChecked.countDown();
              return latest.get();
            });
    try (PaimonCatalogWrapper wrapper = new PaimonCatalogWrapper("test", new Options(), catalog)) {
      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        Future<Boolean> commit =
            executor.submit(
                () -> wrapper.commitTable(identifier, "table-id", null, snapshot("writer")));
        assertTrue(writerReady.await(5, TimeUnit.SECONDS));
        Future<PaimonCatalogException> rollback =
            executor.submit(
                () -> {
                  rollbackStarted.countDown();
                  return assertThrows(
                      PaimonCatalogException.class,
                      () -> wrapper.rollbackTable(identifier, 1L, 0L));
                });
        assertTrue(commit.get(5, TimeUnit.SECONDS));
        assertEquals(
            PaimonCatalogException.Reason.INVALID, rollback.get(5, TimeUnit.SECONDS).reason());
        verify(table, never()).rollbackTo(1L);
      } finally {
        executor.shutdownNow();
      }
    }
  }

  private static Schema schema() {
    return new Schema(
        List.of(new DataField(0, "id", DataTypes.INT())), List.of(), List.of(), Map.of(), null);
  }

  private static Snapshot snapshot(String writer) {
    return new Snapshot(
        1L,
        0L,
        "base",
        0L,
        "delta",
        0L,
        null,
        null,
        null,
        writer,
        null,
        0L,
        Snapshot.CommitKind.APPEND,
        0L,
        0L,
        0L,
        null,
        null,
        null,
        Map.of(),
        null,
        null);
  }
}
