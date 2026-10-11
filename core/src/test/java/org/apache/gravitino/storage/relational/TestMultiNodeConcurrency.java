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
package org.apache.gravitino.storage.relational;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * Tests multi-node concurrency scenarios (#13182) against a shared relational backend: external
 * drop/recreate ABA, rename racing with drop, concurrent import collision, child creation racing
 * with non-force parent drop, and cross-node cache invalidation.
 */
public class TestMultiNodeConcurrency extends TestJDBCBackend {

  private static final String METALAKE = "multi_metalake";
  private static final String CATALOG = "multi_catalog";
  private static final String SCHEMA = "multi_schema";

  private RelationalEntityStore storeA;
  private RelationalEntityStore storeB;
  private CaffeineEntityCache cacheA;
  private CaffeineEntityCache cacheB;
  private ExecutorService executor;

  /**
   * Initializes two server store instances sharing the backend.
   *
   * @throws Exception on initialization error.
   */
  @BeforeEach
  public void setUpStores() throws Exception {
    Config config = new Config() {};
    cacheA = new CaffeineEntityCache(config);
    cacheB = new CaffeineEntityCache(config);

    storeA = new RelationalEntityStore();
    FieldUtils.writeField(storeA, "backend", backend, true);
    FieldUtils.writeField(storeA, "cache", cacheA, true);

    storeB = new RelationalEntityStore();
    FieldUtils.writeField(storeB, "backend", backend, true);
    FieldUtils.writeField(storeB, "cache", cacheB, true);

    executor = Executors.newFixedThreadPool(4);
  }

  /** Cleans up resources after each test. */
  @AfterEach
  public void tearDown() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  private void setUpHierarchy() throws IOException {
    createParentEntities(METALAKE, CATALOG, SCHEMA, AUDIT_INFO);
  }

  /**
   * Tests external drop/recreate ABA concurrency between Node A and Node B.
   *
   * @throws IOException on error.
   */
  @TestTemplate
  public void testExternalDropRecreateABAConcurrency() throws IOException {
    setUpHierarchy();
    Namespace tableNs = Namespace.of(METALAKE, CATALOG, SCHEMA);
    String tableName = "table_aba";
    NameIdentifier tableIdent = NameIdentifier.of(tableNs, tableName);

    long idA = 5001L;
    TableEntity tableA = createTableEntity(idA, tableNs, tableName, AUDIT_INFO);
    storeA.put(tableA, false);

    // Node A observes table A
    TableEntity observedByA = storeA.get(tableIdent, EntityType.TABLE, TableEntity.class);
    assertEquals(idA, observedByA.id());

    // Node B drops table A externally and recreates replacement table B with idB
    long idB = 5002L;
    storeB.delete(tableIdent, EntityType.TABLE, false);
    TableEntity tableB = createTableEntity(idB, tableNs, tableName, AUDIT_INFO);
    storeB.put(tableB, false);

    // Node A attempts to drop table A using observed identity idA
    assertThrows(
        OptimisticLockException.class,
        () -> storeA.delete(tableIdent, EntityType.TABLE, false, observedByA.id(), null));

    // Replacement table B survives untouched on both nodes
    TableEntity current = storeB.get(tableIdent, EntityType.TABLE, TableEntity.class);
    assertEquals(idB, current.id());
  }

  /**
   * Tests that if a table was unobserved initially, concurrent registration under the same name
   * survives when the dropping node does not delete without verified identity.
   *
   * @throws IOException on error.
   */
  @TestTemplate
  public void testInitiallyUnobservedDropPreservesConcurrentRegistration() throws IOException {
    setUpHierarchy();
    Namespace tableNs = Namespace.of(METALAKE, CATALOG, SCHEMA);
    String tableName = "table_unobserved_node_race";
    NameIdentifier tableIdent = NameIdentifier.of(tableNs, tableName);

    // Node A observes that the table does NOT exist in the store initially
    assertFalse(storeA.exists(tableIdent, EntityType.TABLE));

    // Concurrently, Node B registers replacement table B with idB
    long idB = 5003L;
    TableEntity tableB = createTableEntity(idB, tableNs, tableName, AUDIT_INFO);
    storeB.put(tableB, false);

    // Node A must not delete without verified identity. Stale delete with unobserved id fails:
    assertThrows(
        OptimisticLockException.class,
        () -> storeA.delete(tableIdent, EntityType.TABLE, false, 5000L, null));

    // Replacement table B survives untouched on both nodes
    TableEntity current = storeB.get(tableIdent, EntityType.TABLE, TableEntity.class);
    assertNotNull(current);
    assertEquals(idB, current.id());
  }

  /**
   * Tests table rename racing with drop between Node A and Node B.
   *
   * @throws IOException on error.
   */
  @TestTemplate
  public void testRenameRacingWithDrop() throws IOException {
    setUpHierarchy();
    Namespace tableNs = Namespace.of(METALAKE, CATALOG, SCHEMA);
    String originalName = "table_to_rename";
    String renamedName = "table_renamed";
    NameIdentifier originalIdent = NameIdentifier.of(tableNs, originalName);
    NameIdentifier renamedIdent = NameIdentifier.of(tableNs, renamedName);

    long tableId = 6001L;
    TableEntity original = createTableEntity(tableId, tableNs, originalName, AUDIT_INFO);
    storeA.put(original, false);

    // Node A renames original table to renamedName
    storeA.update(
        originalIdent,
        TableEntity.class,
        EntityType.TABLE,
        entity ->
            TableEntity.builder()
                .withId(entity.id())
                .withName(renamedName)
                .withNamespace(entity.namespace())
                .withAuditInfo(entity.auditInfo())
                .build(),
        tableId);

    // Node B attempts to drop using the stale original name
    assertFalse(storeB.delete(originalIdent, EntityType.TABLE, false, tableId, null));

    // The renamed entity survives intact
    TableEntity renamedEntity = storeB.get(renamedIdent, EntityType.TABLE, TableEntity.class);
    assertNotNull(renamedEntity);
    assertEquals(tableId, renamedEntity.id());
    assertEquals(renamedName, renamedEntity.name());
  }

  /**
   * Tests concurrent imports attempting to claim the same identifier / entity ID.
   *
   * @throws Exception on error.
   */
  @TestTemplate
  public void testConcurrentImportCollision() throws Exception {
    setUpHierarchy();
    Namespace tableNs = Namespace.of(METALAKE, CATALOG, SCHEMA);
    long sharedId = 7001L;

    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger collisionCount = new AtomicInteger(0);

    Runnable task1 =
        () -> {
          try {
            startLatch.await();
            TableEntity t1 = createTableEntity(sharedId, tableNs, "concurrent_t1", AUDIT_INFO);
            storeA.put(t1, false);
            successCount.incrementAndGet();
          } catch (EntityAlreadyExistsException e) {
            collisionCount.incrementAndGet();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };

    Runnable task2 =
        () -> {
          try {
            startLatch.await();
            TableEntity t2 = createTableEntity(sharedId, tableNs, "concurrent_t2", AUDIT_INFO);
            storeB.put(t2, false);
            successCount.incrementAndGet();
          } catch (EntityAlreadyExistsException e) {
            collisionCount.incrementAndGet();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };

    Future<?> f1 = executor.submit(task1);
    Future<?> f2 = executor.submit(task2);

    startLatch.countDown();
    f1.get(10, TimeUnit.SECONDS);
    f2.get(10, TimeUnit.SECONDS);

    // Exactly one must succeed, and the other must be rejected with EntityAlreadyExistsException
    assertEquals(1, successCount.get());
    assertEquals(1, collisionCount.get());
  }

  /**
   * Tests that an existing child schema blocks non-force parent catalog deletion.
   *
   * @throws Exception on error.
   */
  @TestTemplate
  public void testExistingChildSchemaBlocksParentCatalogNonForceDrop() throws Exception {
    createAndInsertMakeLake(METALAKE);
    createAndInsertCatalog(METALAKE, CATALOG);
    NameIdentifier catalogIdent = NameIdentifier.of(METALAKE, CATALOG);
    Namespace schemaNs = Namespace.of(METALAKE, CATALOG);

    SchemaEntity childSchema = createSchemaEntity(8001L, schemaNs, "racing_schema", AUDIT_INFO);
    storeA.put(childSchema, false);

    // Non-force drop must fail because child schema exists
    assertThrows(
        NonEmptyEntityException.class,
        () -> storeB.delete(catalogIdent, EntityType.CATALOG, false));

    // Child schema is still present
    assertNotNull(
        storeA.get(
            NameIdentifier.of(schemaNs, "racing_schema"), EntityType.SCHEMA, SchemaEntity.class));
  }

  /**
   * Tests that an existing child catalog blocks non-force parent metalake deletion.
   *
   * @throws Exception on error.
   */
  @TestTemplate
  public void testExistingChildCatalogBlocksParentMetalakeNonForceDrop() throws Exception {
    createAndInsertMakeLake(METALAKE);
    NameIdentifier metalakeIdent = NameIdentifier.of(METALAKE);
    Namespace catalogNs = Namespace.of(METALAKE);

    CatalogEntity childCatalog = createCatalog(8002L, catalogNs, "racing_catalog", AUDIT_INFO);
    storeA.put(childCatalog, false);

    // Non-force drop must fail because child catalog exists
    assertThrows(
        NonEmptyEntityException.class,
        () -> storeB.delete(metalakeIdent, EntityType.METALAKE, false));

    // Child catalog is still present
    assertNotNull(
        storeA.get(
            NameIdentifier.of(catalogNs, "racing_catalog"),
            EntityType.CATALOG,
            CatalogEntity.class));
  }

  /**
   * Tests concurrent child catalog creation racing with non-force parent metalake deletion across
   * nodes, verifying that database row locking strictly serializes the two transactions so that
   * either child creation commits and blocks the drop (NonEmptyEntityException), or parent drop
   * commits and child creation fails (NoSuchEntityException).
   *
   * @throws Exception on error.
   */
  @TestTemplate
  public void testConcurrentChildCatalogCreateAndParentMetalakeNonForceDropRace() throws Exception {
    createAndInsertMakeLake(METALAKE);
    NameIdentifier metalakeIdent = NameIdentifier.of(METALAKE);
    Namespace catalogNs = Namespace.of(METALAKE);
    CatalogEntity childCatalog = createCatalog(8003L, catalogNs, "racing_cat", AUDIT_INFO);

    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicReference<Throwable> putError = new AtomicReference<>();
    AtomicReference<Throwable> dropError = new AtomicReference<>();

    Future<?> fPut =
        executor.submit(
            () -> {
              try {
                startLatch.await();
                storeA.put(childCatalog, false);
              } catch (Throwable t) {
                putError.set(t);
              }
            });

    Future<?> fDrop =
        executor.submit(
            () -> {
              try {
                startLatch.await();
                storeB.delete(metalakeIdent, EntityType.METALAKE, false);
              } catch (Throwable t) {
                dropError.set(t);
              }
            });

    startLatch.countDown();
    fPut.get(10, TimeUnit.SECONDS);
    fDrop.get(10, TimeUnit.SECONDS);

    // Serialized by DB row lock: exactly one transaction wins the metalake lock.
    // If put wins: put succeeds, drop fails with NonEmptyEntityException.
    // If drop wins: drop succeeds, put fails with NoSuchEntityException.
    if (putError.get() == null) {
      assertTrue(dropError.get() instanceof NonEmptyEntityException);
      assertTrue(storeA.exists(NameIdentifier.of(catalogNs, "racing_cat"), EntityType.CATALOG));
      assertTrue(storeA.exists(metalakeIdent, EntityType.METALAKE));
    } else {
      assertTrue(putError.get() instanceof NoSuchEntityException);
      assertNull(dropError.get());
      assertFalse(storeB.exists(metalakeIdent, EntityType.METALAKE));
    }
  }

  /**
   * Tests concurrent catalog drops between Node A and Node B across nodes.
   *
   * @throws Exception on error.
   */
  @TestTemplate
  public void testConcurrentCatalogDropsAcrossNodes() throws Exception {
    createAndInsertMakeLake(METALAKE);
    createAndInsertCatalog(METALAKE, CATALOG);
    NameIdentifier catalogIdent = NameIdentifier.of(METALAKE, CATALOG);

    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger falseCount = new AtomicInteger(0);

    Runnable dropA =
        () -> {
          try {
            startLatch.await();
            if (storeA.delete(catalogIdent, EntityType.CATALOG, true)) {
              successCount.incrementAndGet();
            } else {
              falseCount.incrementAndGet();
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };

    Runnable dropB =
        () -> {
          try {
            startLatch.await();
            if (storeB.delete(catalogIdent, EntityType.CATALOG, true)) {
              successCount.incrementAndGet();
            } else {
              falseCount.incrementAndGet();
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };

    Future<?> f1 = executor.submit(dropA);
    Future<?> f2 = executor.submit(dropB);

    startLatch.countDown();
    f1.get(10, TimeUnit.SECONDS);
    f2.get(10, TimeUnit.SECONDS);

    // Exactly one drop succeeds and returns true; the second returns false idempotently
    assertEquals(1, successCount.get());
    assertEquals(1, falseCount.get());
    assertFalse(storeA.exists(catalogIdent, EntityType.CATALOG));
    assertFalse(storeB.exists(catalogIdent, EntityType.CATALOG));
  }

  /**
   * Tests concurrent metalake drops between Node A and Node B across nodes.
   *
   * @throws Exception on error.
   */
  @TestTemplate
  public void testConcurrentMetalakeDropsAcrossNodes() throws Exception {
    createAndInsertMakeLake(METALAKE);
    NameIdentifier metalakeIdent = NameIdentifier.of(METALAKE);

    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger falseCount = new AtomicInteger(0);

    Runnable dropA =
        () -> {
          try {
            startLatch.await();
            if (storeA.delete(metalakeIdent, EntityType.METALAKE, true)) {
              successCount.incrementAndGet();
            } else {
              falseCount.incrementAndGet();
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };

    Runnable dropB =
        () -> {
          try {
            startLatch.await();
            if (storeB.delete(metalakeIdent, EntityType.METALAKE, true)) {
              successCount.incrementAndGet();
            } else {
              falseCount.incrementAndGet();
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };

    Future<?> f1 = executor.submit(dropA);
    Future<?> f2 = executor.submit(dropB);

    startLatch.countDown();
    f1.get(10, TimeUnit.SECONDS);
    f2.get(10, TimeUnit.SECONDS);

    // Exactly one drop succeeds and returns true; the second returns false idempotently
    assertEquals(1, successCount.get());
    assertEquals(1, falseCount.get());
    assertFalse(storeA.exists(metalakeIdent, EntityType.METALAKE));
    assertFalse(storeB.exists(metalakeIdent, EntityType.METALAKE));
  }

  /**
   * Tests cross-node cache invalidation through change log listener.
   *
   * @throws IOException on error.
   */
  @TestTemplate
  public void testCacheInvalidationAcrossNodes() throws IOException {
    setUpHierarchy();
    Namespace tableNs = Namespace.of(METALAKE, CATALOG, SCHEMA);
    String tableName = "cache_coherence_tbl";
    NameIdentifier tableIdent = NameIdentifier.of(tableNs, tableName);

    long tableId = 9001L;
    TableEntity initialTable = createTableEntity(tableId, tableNs, tableName, AUDIT_INFO);
    storeA.put(initialTable, false);

    // Populate cache on Node A
    TableEntity fromA = storeA.get(tableIdent, EntityType.TABLE, TableEntity.class);
    assertEquals(tableId, fromA.id());
    assertTrue(cacheA.contains(tableIdent, EntityType.TABLE));

    // Node B drops the table
    storeB.delete(tableIdent, EntityType.TABLE, false);

    // Change log record is polled on Node A
    List<EntityChangeRecord> changes =
        SessionUtils.doWithCommitAndFetchResult(
            EntityChangeLogMapper.class, mapper -> mapper.selectEntityChanges(0L, 100));
    assertFalse(changes.isEmpty());

    EntityCacheChangeLogListener listener = storeA.newCacheChangeLogListener();
    listener.onEntityChange(changes);

    // Node A's cache must be invalidated
    assertFalse(cacheA.contains(tableIdent, EntityType.TABLE));
  }
}
