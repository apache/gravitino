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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityWriteIntent;
import org.apache.gravitino.EntityWriteSnapshot;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.Mockito;

/** Verifies write intents against independent connections to each relational backend. */
public class TestEntityWriteIntents extends TestJDBCBackend {
  private static final Namespace NAMESPACE = Namespace.of("metalake", "catalog", "schema");

  /** Checks conditional writes for each supported registration type. */
  @TestTemplate
  public void testRegistrationSnapshotTypes() throws Exception {
    EntityStore store = newStoreWithParents();
    SchemaEntity schema =
        backend.get(NameIdentifier.of("metalake", "catalog", "schema"), Entity.EntityType.SCHEMA);
    assertReconcileAdvancesVersion(store, schema, SchemaEntity.class);
    TopicEntity topic = createTopicEntity(1001L, NAMESPACE, "topic", AUDIT_INFO);
    store.put(topic, EntityWriteIntent.CREATE);
    assertReconcileAdvancesVersion(store, topic, TopicEntity.class);
    ViewEntity view = createViewEntity(1002L, NAMESPACE, "view");
    store.put(view, EntityWriteIntent.CREATE);
    assertReconcileAdvancesVersion(store, view, ViewEntity.class);
  }

  /** Checks strict and idempotent creates preserve the existing entity. */
  @TestTemplate
  public void testCreateAndCreateIfAbsent() throws Exception {
    EntityStore store = newStoreWithParents();
    TableEntity first = table(1001L, "table", "winner");
    store.put(first, EntityWriteIntent.CREATE);
    EntityWriteSnapshot<TableEntity> before = snapshot(store, first);
    TableEntity second = table(1002L, "table", "loser");
    Assertions.assertThrows(
        EntityAlreadyExistsException.class, () -> store.put(second, EntityWriteIntent.CREATE));
    TableEntity actual = store.put(second, EntityWriteIntent.CREATE_IF_ABSENT);
    Assertions.assertEquals(first.id(), actual.id());
    Assertions.assertEquals("winner", actual.comment());
    Assertions.assertEquals(before.version(), snapshot(store, first).version());
  }

  /** Checks imports cannot replace metadata or copy an existing identity. */
  @TestTemplate
  public void testImportPreservesIdentityAndContents() throws Exception {
    EntityStore store = newStoreWithParents();
    TableEntity first = table(1001L, "table", "winner");
    store.put(first, EntityWriteIntent.IMPORT);
    EntityWriteSnapshot<TableEntity> before = snapshot(store, first);
    TableEntity actual = store.put(table(1001L, "table", "stale"), EntityWriteIntent.IMPORT);
    Assertions.assertEquals("winner", actual.comment());
    Assertions.assertEquals(before.version(), snapshot(store, first).version());
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () -> store.put(table(1002L, "table", "other ID"), EntityWriteIntent.IMPORT));
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () -> store.put(table(1001L, "copied", "copied ID"), EntityWriteIntent.IMPORT));
    Assertions.assertEquals("winner", snapshot(store, first).entity().comment());
    Assertions.assertFalse(
        store.exists(table(1001L, "copied", "").nameIdentifier(), Entity.EntityType.TABLE));
  }

  /** Checks imports cannot reuse a tombstoned identity. */
  @TestTemplate
  public void testImportDoesNotReviveDeletedIdentity() throws Exception {
    EntityStore store = newStoreWithParents();
    TableEntity first = table(1001L, "table", "original");
    store.put(first, EntityWriteIntent.CREATE);
    store.delete(first.nameIdentifier(), first.type());
    Assertions.assertThrows(
        EntityAlreadyExistsException.class, () -> store.put(first, EntityWriteIntent.IMPORT));
    Assertions.assertFalse(store.exists(first.nameIdentifier(), first.type()));
  }

  /** Checks a snapshot refreshes reads cached before acquiring its row lock. */
  @TestTemplate
  public void testSnapshotRefreshesContentsAfterWaitingForRowLock() throws Exception {
    EntityStore store = newStoreWithParents();
    TableEntity first = table(1001L, "table", "old");
    store.put(first, EntityWriteIntent.CREATE);
    RelationalBackend racingBackend = Mockito.spy(backend);
    AtomicInteger reads = new AtomicInteger();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Mockito.doAnswer(
              invocation -> {
                TableEntity result = (TableEntity) invocation.callRealMethod();
                if (reads.getAndIncrement() == 0) {
                  executor
                      .submit(
                          () -> {
                            backend.update(
                                first.nameIdentifier(),
                                first.type(),
                                ignored -> table(1001L, "table", "new"));
                            return null;
                          })
                      .get(30, TimeUnit.SECONDS);
                }
                return result;
              })
          .when(racingBackend)
          .get(first.nameIdentifier(), first.type());
      EntityWriteSnapshot<TableEntity> observed =
          racingBackend.getWriteSnapshot(first.nameIdentifier(), first.type());
      Assertions.assertEquals("new", observed.entity().comment());
      Assertions.assertEquals(snapshot(store, first).version(), observed.version());
      store.put(table(1001L, "table", "reconciled"), EntityWriteIntent.RECONCILE, observed);
    } finally {
      executor.shutdownNow();
      Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  /** Checks a stale version is rejected even after values are restored. */
  @TestTemplate
  public void testReconcileRejectsStaleVersionIncludingAba() throws Exception {
    EntityStore store = newStoreWithParents();
    TableEntity first = table(1001L, "table", "initial");
    store.put(first, EntityWriteIntent.CREATE);
    EntityWriteSnapshot<TableEntity> observed = snapshot(store, first);
    store.put(table(1001L, "table", "updated"), EntityWriteIntent.RECONCILE, observed);
    EntityWriteSnapshot<TableEntity> next = snapshot(store, first);
    Assertions.assertTrue(next.version() > observed.version());
    store.put(first, EntityWriteIntent.RECONCILE, next);
    Assertions.assertThrows(
        OptimisticLockException.class,
        () -> store.put(table(1001L, "table", "stale"), EntityWriteIntent.RECONCILE, observed));
    Assertions.assertEquals("initial", snapshot(store, first).entity().comment());
  }

  /** Checks reconciliation cannot rename, rebind, or revive an entity. */
  @TestTemplate
  public void testReconcileCannotChangeOrReplaceIdentity() throws Exception {
    EntityStore store = newStoreWithParents();
    TableEntity first = table(1001L, "table", "original");
    store.put(first, EntityWriteIntent.CREATE);
    EntityWriteSnapshot<TableEntity> observed = snapshot(store, first);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> store.put(first, EntityWriteIntent.RECONCILE));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> store.put(first, EntityWriteIntent.CREATE, observed));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> store.put(table(1002L, "table", "other"), EntityWriteIntent.RECONCILE, observed));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> store.put(table(1001L, "renamed", "other"), EntityWriteIntent.RECONCILE, observed));
    store.delete(first.nameIdentifier(), first.type());
    TableEntity replacement = table(1002L, "table", "replacement");
    store.put(replacement, EntityWriteIntent.CREATE);
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> store.put(first, EntityWriteIntent.RECONCILE, observed));
    Assertions.assertEquals("replacement", snapshot(store, replacement).entity().comment());
  }

  /** Checks post-insert actions run only for winners and failures roll back metadata. */
  @TestTemplate
  public void testCreateActionRunsOnlyForWinnerAndRollsBackOnFailure() throws Exception {
    EntityStore store = newStoreWithParents();
    TableEntity first = table(1001L, "table", "winner");
    AtomicInteger calls = new AtomicInteger();
    store.put(first, EntityWriteIntent.CREATE, ignored -> calls.incrementAndGet());
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            store.put(
                table(1002L, "table", "loser"),
                EntityWriteIntent.CREATE,
                ignored -> calls.incrementAndGet()));
    Assertions.assertEquals(1, calls.get());
    TableEntity failed = table(1003L, "failed", "failed");
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            store.put(
                failed,
                EntityWriteIntent.CREATE,
                ignored -> {
                  throw new IllegalStateException("storage unavailable");
                }));
    Assertions.assertFalse(store.exists(failed.nameIdentifier(), failed.type()));
    // The failed insert and version rows were rolled back, so even its stable ID is reusable.
    store.put(failed, EntityWriteIntent.CREATE);
  }

  /** Checks independent stores cannot both create the same fileset. */
  @TestTemplate
  public void testIndependentNodesCannotOverwriteConcurrentFilesetCreate() throws Exception {
    EntityStore firstNode = newStoreWithParents();
    EntityStore secondNode = newStore();
    CountDownLatch start = new CountDownLatch(1);
    AtomicInteger actions = new AtomicInteger();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Boolean> first =
          executor.submit(
              () -> createAfterBarrier(firstNode, fileset(1001L, "first"), start, actions));
      Future<Boolean> second =
          executor.submit(
              () -> createAfterBarrier(secondNode, fileset(1002L, "second"), start, actions));
      start.countDown();
      boolean firstWon = first.get(30, TimeUnit.SECONDS);
      boolean secondWon = second.get(30, TimeUnit.SECONDS);
      Assertions.assertNotEquals(firstWon, secondWon);
      Assertions.assertEquals(1, actions.get());
      FilesetEntity winner =
          backend.get(NameIdentifier.of(NAMESPACE, "fileset"), Entity.EntityType.FILESET);
      Assertions.assertEquals(firstWon ? 1001L : 1002L, winner.id());
      Assertions.assertEquals(firstWon ? "first" : "second", winner.comment());
      Assertions.assertEquals(
          Map.of("default", firstWon ? "/first" : "/second"), winner.storageLocations());
    } finally {
      executor.shutdownNow();
      Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  /** Checks the deprecated boolean API retains its compatibility behavior. */
  @TestTemplate
  @SuppressWarnings("deprecation")
  public void testLegacyBooleanRetainsOverwriteBehavior() throws Exception {
    EntityStore store = newStoreWithParents();
    TableEntity first = table(1001L, "table", "original");
    store.put(first, false);
    store.put(table(1001L, "table", "legacy update"), true);
    Assertions.assertEquals("legacy update", snapshot(store, first).entity().comment());
  }

  private <E extends Entity & HasIdentifier> void assertReconcileAdvancesVersion(
      EntityStore store, E entity, Class<E> clazz) throws IOException {
    EntityWriteSnapshot<E> observed =
        store.getWriteSnapshot(entity.nameIdentifier(), entity.type(), clazz);
    store.put(entity, EntityWriteIntent.RECONCILE, observed);
    Assertions.assertTrue(
        store.getWriteSnapshot(entity.nameIdentifier(), entity.type(), clazz).version()
            > observed.version());
    Assertions.assertThrows(
        OptimisticLockException.class,
        () -> store.put(entity, EntityWriteIntent.RECONCILE, observed));
  }

  private EntityStore newStoreWithParents() throws Exception {
    createParentEntities("metalake", "catalog", "schema", AUDIT_INFO);
    return newStore();
  }

  private EntityStore newStore() throws IllegalAccessException {
    RelationalEntityStore store = new RelationalEntityStore();
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", Mockito.mock(EntityCache.class), true);
    return store;
  }

  private FilesetEntity fileset(long id, String comment) {
    return FilesetEntity.builder()
        .withId(id)
        .withName("fileset")
        .withNamespace(NAMESPACE)
        .withFilesetType(Fileset.Type.MANAGED)
        .withComment(comment)
        .withStorageLocations(Map.of("default", "/" + comment))
        .withProperties(Map.of("owner", comment))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private TableEntity table(long id, String name, String comment) {
    return TableEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NAMESPACE)
        .withComment(comment)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private EntityWriteSnapshot<TableEntity> snapshot(EntityStore store, TableEntity table)
      throws IOException {
    return store.getWriteSnapshot(table.nameIdentifier(), table.type(), TableEntity.class);
  }

  private <E extends Entity & HasIdentifier> boolean createAfterBarrier(
      EntityStore store, E table, CountDownLatch start, AtomicInteger actions) throws Exception {
    Assertions.assertTrue(start.await(30, TimeUnit.SECONDS));
    try {
      store.put(table, EntityWriteIntent.CREATE, ignored -> actions.incrementAndGet());
      return true;
    } catch (EntityAlreadyExistsException expected) {
      return false;
    }
  }
}
