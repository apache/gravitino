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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * Verifies the contract documented on {@link EntityStore} against {@link RelationalEntityStore} on
 * every relational backend.
 */
public class TestEntityStoreContract extends TestJDBCBackend {

  private static final String METALAKE = "contract_metalake";
  private static final String CATALOG = "contract_catalog";

  private Namespace schemaNamespace;
  private RelationalEntityStore store;

  @BeforeEach
  public void prepareStore() throws IOException, IllegalAccessException {
    createAndInsertMakeLake(METALAKE);
    createAndInsertCatalog(METALAKE, CATALOG);
    schemaNamespace = Namespace.of(METALAKE, CATALOG);
    Config config = new Config(false) {};
    store = newStore(new NoOpsCache(config));
  }

  @TestTemplate
  public void testPutWithoutOverwriteRejectsTakenName() throws IOException {
    store.put(newSchema("taken"));

    assertThrows(EntityAlreadyExistsException.class, () -> store.put(newSchema("taken")));
    assertThrows(EntityAlreadyExistsException.class, () -> store.put(newSchema("taken"), false));
  }

  @TestTemplate
  public void testPutUnderMissingParentThrowsNoSuchEntity() {
    SchemaEntity orphan =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE, "missing_catalog"),
            "orphan",
            AUDIT_INFO);

    assertThrows(NoSuchEntityException.class, () -> store.put(orphan));
  }

  @TestTemplate
  public void testUpdateCallsUpdaterOnceWithStoredEntity() throws IOException {
    SchemaEntity schema = newSchema("updated_once");
    store.put(schema);
    AtomicInteger calls = new AtomicInteger();

    SchemaEntity updated =
        store.update(
            schema.nameIdentifier(),
            SchemaEntity.class,
            Entity.EntityType.SCHEMA,
            current -> {
              calls.incrementAndGet();
              assertEquals(schema.id(), current.id());
              return withComment(current, "changed");
            });

    assertEquals(1, calls.get());
    assertEquals("changed", updated.comment());
  }

  @TestTemplate
  public void testUpdaterFailureWritesNothing() throws IOException {
    SchemaEntity schema = newSchema("updater_failure");
    store.put(schema);

    assertThrows(
        RuntimeException.class,
        () ->
            store.update(
                schema.nameIdentifier(),
                SchemaEntity.class,
                Entity.EntityType.SCHEMA,
                current -> {
                  throw new IllegalStateException("abort");
                }));

    assertEquals(schema.comment(), storedSchema(schema.nameIdentifier()).comment());
  }

  @TestTemplate
  public void testUpdateRejectsIdChange() throws IOException {
    SchemaEntity schema = newSchema("id_change");
    store.put(schema);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            store.update(
                schema.nameIdentifier(),
                SchemaEntity.class,
                Entity.EntityType.SCHEMA,
                current ->
                    SchemaEntity.builder()
                        .withId(current.id() + 1)
                        .withName(current.name())
                        .withNamespace(current.namespace())
                        .withComment("changed")
                        .withProperties(current.properties())
                        .withAuditInfo(current.auditInfo())
                        .build()));

    SchemaEntity stored = storedSchema(schema.nameIdentifier());
    assertEquals(schema.id(), stored.id());
    assertEquals(schema.comment(), stored.comment());
  }

  @TestTemplate
  public void testRenameToTakenNameFails() throws IOException {
    SchemaEntity source = newSchema("rename_source");
    SchemaEntity target = newSchema("rename_target");
    store.put(source);
    store.put(target);

    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            store.update(
                source.nameIdentifier(),
                SchemaEntity.class,
                Entity.EntityType.SCHEMA,
                current -> withName(current, target.name())));
  }

  @TestTemplate
  public void testUpdateOfMissingEntityThrowsNoSuchEntity() {
    NameIdentifier missing = NameIdentifier.of(schemaNamespace, "missing");

    assertThrows(
        NoSuchEntityException.class,
        () ->
            store.update(
                missing, SchemaEntity.class, Entity.EntityType.SCHEMA, current -> current));
  }

  @TestTemplate
  public void testUpdateLosingToConcurrentWriteThrowsOptimisticLock() throws Exception {
    SchemaEntity schema = newSchema("concurrent_update");
    store.put(schema);
    NameIdentifier ident = schema.nameIdentifier();

    assertThrows(
        OptimisticLockException.class,
        () ->
            store.update(
                ident,
                SchemaEntity.class,
                Entity.EntityType.SCHEMA,
                current -> {
                  // Another writer, on its own connection, commits between this update's read and
                  // its write.
                  try {
                    CompletableFuture.runAsync(
                            () -> {
                              try {
                                store.update(
                                    ident,
                                    SchemaEntity.class,
                                    Entity.EntityType.SCHEMA,
                                    other -> withComment(other, "winner"));
                              } catch (IOException e) {
                                throw new UncheckedIOException(e);
                              }
                            })
                        .get(30, TimeUnit.SECONDS);
                  } catch (Exception e) {
                    throw new IllegalStateException("The concurrent writer did not commit", e);
                  }
                  return withComment(current, "loser");
                }));

    assertEquals("winner", storedSchema(ident).comment());
  }

  @TestTemplate
  public void testUpdateLosingToConcurrentDeleteThrowsNoSuchEntity() throws Exception {
    SchemaEntity schema = newSchema("concurrent_delete");
    store.put(schema);
    NameIdentifier ident = schema.nameIdentifier();

    assertThrows(
        NoSuchEntityException.class,
        () ->
            store.update(
                ident,
                SchemaEntity.class,
                Entity.EntityType.SCHEMA,
                current -> {
                  // Another writer, on its own connection, deletes the entity between this
                  // update's read and its write.
                  try {
                    assertTrue(
                        CompletableFuture.supplyAsync(
                                () -> {
                                  try {
                                    return store.delete(ident, Entity.EntityType.SCHEMA);
                                  } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                  }
                                })
                            .get(30, TimeUnit.SECONDS));
                  } catch (Exception e) {
                    throw new IllegalStateException("The concurrent delete did not commit", e);
                  }
                  return withComment(current, "loser");
                }));

    assertFalse(store.exists(ident, Entity.EntityType.SCHEMA));
  }

  @TestTemplate
  public void testDeleteOfMissingEntityReturnsFalse() throws IOException {
    assertFalse(
        store.delete(NameIdentifier.of(schemaNamespace, "missing"), Entity.EntityType.SCHEMA));
    assertFalse(
        store.delete(
            NameIdentifier.of(Namespace.of(METALAKE, CATALOG, "missing"), "missing"),
            Entity.EntityType.TABLE,
            true));
  }

  @TestTemplate
  public void testNonCascadeDeleteOfNonEmptySchemaFails() throws IOException {
    SchemaEntity schema = newSchema("non_empty");
    store.put(schema);
    createAndInsertTableEntity(Namespace.of(METALAKE, CATALOG, schema.name()), "child");

    assertThrows(
        NonEmptyEntityException.class,
        () -> store.delete(schema.nameIdentifier(), Entity.EntityType.SCHEMA, false));
    assertTrue(store.exists(schema.nameIdentifier(), Entity.EntityType.SCHEMA));

    assertTrue(store.delete(schema.nameIdentifier(), Entity.EntityType.SCHEMA, true));
    assertFalse(store.exists(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
  }

  @TestTemplate
  public void testBatchGetLeavesOutMissingEntities() throws IOException {
    SchemaEntity first = newSchema("batch_first");
    SchemaEntity second = newSchema("batch_second");
    store.put(first);
    store.put(second);

    List<SchemaEntity> found =
        store.batchGet(
            ImmutableList.of(
                second.nameIdentifier(),
                NameIdentifier.of(schemaNamespace, "batch_missing"),
                first.nameIdentifier()),
            Entity.EntityType.SCHEMA,
            SchemaEntity.class);

    assertEquals(
        Set.of(first.id(), second.id()),
        found.stream().map(SchemaEntity::id).collect(Collectors.toSet()));
    assertEquals(2, found.size());
  }

  @TestTemplate
  public void testBatchGetRejectsMixedNamespaces() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            store.batchGet(
                ImmutableList.of(
                    NameIdentifier.of(schemaNamespace, "first"),
                    NameIdentifier.of(Namespace.of(METALAKE, "other_catalog"), "second")),
                Entity.EntityType.SCHEMA,
                SchemaEntity.class));
  }

  @TestTemplate
  public void testRenameDropsStaleCacheEntryOfNewName() throws Exception {
    EntityCache cache = new CaffeineEntityCache(new Config() {});
    RelationalEntityStore cachedStore = newStore(cache);
    SchemaEntity schema = newSchema("rename_from");
    cachedStore.put(schema);

    // Another server removed an entity named "rename_to" that this server still has cached.
    SchemaEntity stale = newSchema("rename_to");
    cache.put(stale);

    cachedStore.update(
        schema.nameIdentifier(),
        SchemaEntity.class,
        Entity.EntityType.SCHEMA,
        current -> withName(current, "rename_to"));

    SchemaEntity renamed =
        cachedStore.get(
            NameIdentifier.of(schemaNamespace, "rename_to"),
            Entity.EntityType.SCHEMA,
            SchemaEntity.class);
    assertEquals(schema.id(), renamed.id());
  }

  @TestTemplate
  @SuppressWarnings("deprecation")
  public void testExecuteInTransactionIsUnsupported() {
    assertThrows(UnsupportedOperationException.class, () -> store.executeInTransaction(() -> null));
  }

  private RelationalEntityStore newStore(EntityCache cache) throws IllegalAccessException {
    RelationalEntityStore relationalStore = new RelationalEntityStore();
    FieldUtils.writeField(relationalStore, "backend", backend, true);
    FieldUtils.writeField(relationalStore, "cache", cache, true);
    return relationalStore;
  }

  private SchemaEntity newSchema(String name) {
    return createSchemaEntity(
        RandomIdGenerator.INSTANCE.nextId(), schemaNamespace, name, AUDIT_INFO);
  }

  private SchemaEntity storedSchema(NameIdentifier ident) throws IOException {
    return backend.get(ident, Entity.EntityType.SCHEMA);
  }

  private static SchemaEntity withComment(SchemaEntity schema, String comment) {
    return SchemaEntity.builder()
        .withId(schema.id())
        .withName(schema.name())
        .withNamespace(schema.namespace())
        .withComment(comment)
        .withProperties(schema.properties())
        .withAuditInfo(schema.auditInfo())
        .build();
  }

  private static SchemaEntity withName(SchemaEntity schema, String name) {
    return SchemaEntity.builder()
        .withId(schema.id())
        .withName(name)
        .withNamespace(schema.namespace())
        .withComment(schema.comment())
        .withProperties(schema.properties())
        .withAuditInfo(schema.auditInfo())
        .build();
  }
}
