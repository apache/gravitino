/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.lakehouse.lance;

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_STORAGE_OPTIONS_PREFIX;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_TABLE_DECLARED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.Executable;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;
import org.mockito.Mockito;

/**
 * Real multi-threaded reproduction of the repair-on-load optimistic-lock race behind #11891. Unlike
 * {@code TestLanceTableOperations#testLoadTableSurvivesConcurrentRepairVersionRace} (a
 * deterministic mock that throws one scripted {@code IOException}), this test drives {@link
 * LanceTableOperations#loadTable} from several threads at once against a {@link CasEntityStore}
 * that models the production relational store's compare-and-set semantics faithfully: every {@code
 * update} bumps a version guarded by the base version, so concurrent updates from the same base
 * conflict and exactly one wins per generation — just like {@code TableMetaService.updateTable}
 * ({@code UPDATE ... WHERE current_version = old}).
 *
 * <p>Before the CAS retry, the loser of the race got {@code IOException("Failed to update the
 * entity")}, rethrown as a fatal {@code RuntimeException} (HTTP 500). This test asserts every
 * concurrent load returns the repaired table instead.
 */
public class TestLanceConcurrentRepairStress {

  @TempDir private java.nio.file.Path tempDir;

  private static final NameIdentifier IDENT = NameIdentifier.of("schema", "table");

  @Test
  public void testConcurrentRepairLoadsSurviveCasContention() throws Exception {
    // The reported race is "two loads repair the same table at once". Also drive a small herd to
    // confirm the bounded CAS retry stays robust beyond the minimal two-load case.
    runStress(2, 3000);
    runStress(8, 2000);
  }

  private void runStress(int concurrency, int iterations) throws Exception {
    String location = tempDir.resolve("cas-race-" + concurrency).toString();
    Map<String, String> storageOptions = Map.of("endpoint", "http://endpoint");
    Schema datasetSchema =
        new Schema(
            List.of(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("name", new ArrowType.Utf8())));

    ManagedSchemaOperations schemaOps = mock(ManagedSchemaOperations.class);
    IdGenerator idGenerator = mock(IdGenerator.class);
    AtomicLong idSeq = new AtomicLong(1000);
    when(idGenerator.nextId()).thenAnswer(invocation -> idSeq.incrementAndGet());

    ExecutorService pool = Executors.newFixedThreadPool(concurrency);
    UserPrincipal user = new UserPrincipal("tester");
    int failures = 0;
    try {
      for (int iter = 0; iter < iterations; iter++) {
        // Fresh stale (declared, empty-schema) entity every iteration so each round starts from the
        // pre-repair state that triggers repairTableMetadata on every concurrent load.
        TableEntity stale =
            tableEntity(
                List.of(),
                Map.of(
                    Table.PROPERTY_LOCATION,
                    location,
                    LANCE_TABLE_DECLARED,
                    "true",
                    LANCE_STORAGE_OPTIONS_PREFIX + "endpoint",
                    "http://endpoint"));
        CasEntityStore store = new CasEntityStore(stale);
        LanceTableOperations ops = spy(new LanceTableOperations(store, schemaOps, idGenerator));
        Dataset dataset = mock(Dataset.class);
        when(dataset.version()).thenReturn(8L);
        when(dataset.getSchema()).thenReturn(datasetSchema);
        Mockito.doReturn(dataset).when(ops).openDataset(location, storageOptions);

        CyclicBarrier barrier = new CyclicBarrier(concurrency);
        List<Future<Table>> futures = new java.util.ArrayList<>(concurrency);
        for (int t = 0; t < concurrency; t++) {
          Callable<Table> task =
              () -> {
                barrier.await();
                return PrincipalUtils.doAs(user, () -> ops.loadTable(IDENT));
              };
          futures.add(pool.submit(task));
        }

        for (Future<Table> future : futures) {
          try {
            Table loaded = future.get(30, TimeUnit.SECONDS);
            Assertions.assertEquals(
                2, loaded.columns().length, "repaired table must expose both columns");
          } catch (ExecutionException e) {
            failures++;
            // Surface the first real failure with its cause for debugging.
            if (failures == 1) {
              throw new AssertionError(
                  "Concurrent repair-on-load failed at iteration "
                      + iter
                      + " with concurrency="
                      + concurrency,
                  e.getCause());
            }
          }
        }
      }
    } finally {
      pool.shutdownNow();
    }
    Assertions.assertEquals(
        0, failures, "no concurrent load should fail (concurrency=" + concurrency + ")");
  }

  private static TableEntity tableEntity(
      List<org.apache.gravitino.meta.ColumnEntity> columns, Map<String, String> properties) {
    return TableEntity.builder()
        .withId(1L)
        .withName(IDENT.name())
        .withNamespace(IDENT.namespace())
        .withComment("comment")
        .withColumns(columns)
        .withProperties(properties)
        .withAuditInfo(
            AuditInfo.builder().withCreator("creator").withCreateTime(Instant.EPOCH).build())
        .build();
  }

  /**
   * In-memory {@link EntityStore} that reproduces the relational store's optimistic-lock CAS:
   * {@code update} reads a versioned snapshot, applies the (idempotent) updater, and commits only
   * if the version has not advanced since the read — otherwise it throws {@code IOException("Failed
   * to update the entity")}, exactly as {@code TableMetaService.updateTable} does when {@code
   * UPDATE ... WHERE current_version = old} matches zero rows. Every commit bumps the version, so
   * even a no-op update invalidates a concurrent update from the same base, matching production.
   */
  private static final class CasEntityStore implements EntityStore {

    private static final class Versioned {
      private final long version;
      private final TableEntity entity;

      Versioned(long version, TableEntity entity) {
        this.version = version;
        this.entity = entity;
      }
    }

    private final AtomicReference<Versioned> ref;

    CasEntityStore(TableEntity initial) {
      this.ref = new AtomicReference<>(new Versioned(0L, initial));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends Entity & HasIdentifier> E get(
        NameIdentifier ident, EntityType entityType, Class<E> type) {
      return (E) ref.get().entity;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends Entity & HasIdentifier> E update(
        NameIdentifier ident, Class<E> type, EntityType entityType, Function<E, E> updater)
        throws IOException {
      Versioned base = ref.get();
      E updated = updater.apply((E) base.entity);
      Versioned next = new Versioned(base.version + 1, (TableEntity) updated);
      if (ref.compareAndSet(base, next)) {
        return updated;
      }
      throw new IOException("Failed to update the entity: " + ident);
    }

    // --- unused surface ---------------------------------------------------

    @Override
    public void initialize(Config config) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists(NameIdentifier ident, EntityType entityType) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <E extends Entity & HasIdentifier> List<E> batchGet(
        List<NameIdentifier> idents, EntityType entityType, Class<E> clazz) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(NameIdentifier ident, EntityType entityType, boolean cascade) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int batchDelete(
        List<Pair<NameIdentifier, EntityType>> entitiesToDelete, boolean cascade) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <E extends Entity & HasIdentifier> void batchPut(List<E> entities, boolean overwritten) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <R, E extends Exception> R executeInTransaction(Executable<R, E> executable) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}
  }
}
