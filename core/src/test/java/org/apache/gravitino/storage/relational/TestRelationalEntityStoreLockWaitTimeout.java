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

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationEdgeTarget;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.exceptions.PersistenceException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

/**
 * Verifies that {@link RelationalEntityStore} reports a write that fails on a lock wait timeout as
 * an {@link OptimisticLockException}, and leaves every other failure unchanged.
 */
public class TestRelationalEntityStoreLockWaitTimeout {

  private static final String METALAKE = "metalake_lwt";
  private static final String CATALOG = "catalog_lwt";
  private static final String SCHEMA = "schema_lwt";
  private static final String USER = "root";
  private static final String PASSWORD = "123456";

  private static final AuditInfo AUDIT_INFO =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

  private static final NameIdentifier TABLE_IDENT =
      NameIdentifier.of(METALAKE, CATALOG, SCHEMA, "t1");

  private static final NameIdentifier DOMAIN_IDENT = NameIdentifierUtil.ofDomain(METALAKE, "d1");

  private RelationalEntityStore store;
  private String dbPath;
  private Object previousConfig;
  private Object previousIdGenerator;

  @AfterEach
  void tearDown() throws Exception {
    // Only the H2 store owns resources; a store over a failing mock backend is just dropped.
    if (dbPath != null) {
      try {
        store.close();
      } finally {
        FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
        FieldUtils.writeField(GravitinoEnv.getInstance(), "idGenerator", previousIdGenerator, true);
        FileUtils.deleteDirectory(new File(dbPath));
        dbPath = null;
      }
    }
    store = null;
  }

  /**
   * Holds the row of a table from another session, as a concurrent table-scoped write does, so that
   * a drop of the table waits on it until H2 aborts the statement.
   */
  @Test
  void testDropWaitingOnHeldTableRowIsConflict() throws Exception {
    String jdbcUrl = initH2Store();
    TableEntity table = createTable();

    OptimisticLockException conflict =
        writeWhileRowHeld(
            jdbcUrl,
            "table_meta",
            "table_id",
            table.id(),
            () -> store.delete(TABLE_IDENT, Entity.EntityType.TABLE));
    Assertions.assertTrue(
        conflict.getMessage().startsWith("The table " + TABLE_IDENT + " could not be changed"),
        conflict.getMessage());

    // The failed drop was rolled back, so the table is still there and a retry succeeds.
    Assertions.assertTrue(store.exists(TABLE_IDENT, Entity.EntityType.TABLE));
    Assertions.assertTrue(store.delete(TABLE_IDENT, Entity.EntityType.TABLE));
    Assertions.assertFalse(store.exists(TABLE_IDENT, Entity.EntityType.TABLE));
  }

  /** An alter goes through the service's own SQL error conversion, which must keep the conflict. */
  @Test
  void testAlterWaitingOnHeldTableRowIsConflict() throws Exception {
    String jdbcUrl = initH2Store();
    TableEntity table = createTable();
    NameIdentifier renamed = NameIdentifier.of(TABLE_IDENT.namespace(), "t1_renamed");

    OptimisticLockException conflict =
        writeWhileRowHeld(
            jdbcUrl,
            "table_meta",
            "table_id",
            table.id(),
            () ->
                store.update(
                    TABLE_IDENT, TableEntity.class, Entity.EntityType.TABLE, this::rename));
    // The service names the entity it writes, which is the new name of a rename.
    Assertions.assertTrue(
        conflict.getMessage().startsWith("The table " + renamed + " could not be changed"),
        conflict.getMessage());

    Assertions.assertTrue(store.exists(TABLE_IDENT, Entity.EntityType.TABLE));
    Assertions.assertFalse(store.exists(renamed, Entity.EntityType.TABLE));
    store.update(TABLE_IDENT, TableEntity.class, Entity.EntityType.TABLE, this::rename);
    Assertions.assertTrue(store.exists(renamed, Entity.EntityType.TABLE));
  }

  /** A create locks its parent row first, so it waits behind a write that holds the schema. */
  @Test
  void testCreateWaitingOnHeldSchemaRowIsConflict() throws Exception {
    String jdbcUrl = initH2Store();
    store.put(metalake(), false);
    store.put(catalog(), false);
    SchemaEntity schema = schema();
    store.put(schema, false);
    TableEntity table = table();

    OptimisticLockException conflict =
        writeWhileRowHeld(
            jdbcUrl, "schema_meta", "schema_id", schema.id(), () -> store.put(table, false));
    Assertions.assertTrue(
        conflict.getMessage().startsWith("The table " + TABLE_IDENT + " could not be changed"),
        conflict.getMessage());

    Assertions.assertFalse(store.exists(TABLE_IDENT, Entity.EntityType.TABLE));
    store.put(table, false);
    Assertions.assertTrue(store.exists(TABLE_IDENT, Entity.EntityType.TABLE));
  }

  /**
   * A tag association locks the table row first, as the issue's example of a table-scoped write.
   */
  @Test
  void testTagAssociationWaitingOnHeldTableRowIsConflict() throws Exception {
    String jdbcUrl = initH2Store();
    TableEntity table = createTable();
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(METALAKE))
            .withAuditInfo(AUDIT_INFO)
            .build();
    store.put(tag, false);
    NameIdentifier[] tags = {tag.nameIdentifier()};

    OptimisticLockException conflict =
        writeWhileRowHeld(jdbcUrl, "table_meta", "table_id", table.id(), () -> associateTags(tags));
    Assertions.assertTrue(
        conflict.getMessage().startsWith("The tags of table " + TABLE_IDENT + " could not"),
        conflict.getMessage());

    Assertions.assertTrue(
        store
            .listEntitiesByRelation(
                SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
                TABLE_IDENT,
                Entity.EntityType.TABLE)
            .isEmpty());
    Assertions.assertEquals(1, associateTags(tags).size());
  }

  /** Verifies every write of the store maps a lock wait timeout of the backend to a conflict. */
  @Test
  void testEveryWriteMapsLockWaitTimeout() throws Exception {
    for (SQLException timeout :
        List.of(
            new SQLException("MySQL lock wait timeout", "40001", 1205),
            new SQLException("MySQL lock wait timeout", "HY000", 1205),
            new SQLException("H2 lock wait timeout", "HYT00", 50200),
            new SQLException("PostgreSQL lock timeout", "55P03"))) {
      PersistenceException failure = new PersistenceException(timeout);
      initFailingStore(failure);
      for (Pair<String, Executable> write : writes()) {
        OptimisticLockException conflict =
            Assertions.assertThrows(
                OptimisticLockException.class, write.getRight(), write.getLeft());
        Assertions.assertSame(failure, conflict.getCause(), write.getLeft());
        Assertions.assertTrue(
            conflict.getMessage().contains("lock wait timeout"), conflict.getMessage());
      }
    }

    // The message names what was written.
    initFailingStore(new PersistenceException(new SQLException("timeout", "HYT00", 50200)));
    OptimisticLockException conflict =
        Assertions.assertThrows(
            OptimisticLockException.class,
            () -> store.delete(TABLE_IDENT, Entity.EntityType.TABLE));
    Assertions.assertTrue(
        conflict.getMessage().startsWith("The table " + TABLE_IDENT + " could not be changed"),
        conflict.getMessage());
  }

  /** Verifies a failure that is not a lock wait timeout reaches the caller unchanged. */
  @Test
  void testOtherFailuresAreUnchanged() throws Exception {
    for (RuntimeException failure :
        List.of(
            // A deadlock points at an inconsistent lock order and is not reported as a conflict.
            new PersistenceException(new SQLException("MySQL deadlock", "40001", 1213)),
            new PersistenceException(new SQLException("PostgreSQL deadlock", "40P01")),
            new PersistenceException(new SQLException("connection lost", "08006")),
            new IllegalStateException("not a SQL failure"),
            // A conflict reported by the backend keeps its own message and is not wrapped again.
            new OptimisticLockException(
                new PersistenceException(new SQLException("timeout", "HYT00", 50200)),
                "already a conflict"))) {
      initFailingStore(failure);
      for (Pair<String, Executable> write : writes()) {
        Throwable thrown = Assertions.assertThrows(Throwable.class, write.getRight());
        Assertions.assertSame(failure, thrown, write.getLeft());
      }
    }
  }

  /** Mixed duplicate-key/timeout chains remain unclassified on direct store write paths. */
  @Test
  void testMixedDuplicateKeyAndTimeoutPreservesBackendFailure() throws Exception {
    for (boolean duplicateFirst : List.of(true, false)) {
      BatchUpdateException batch = new BatchUpdateException("batch failed", new int[0]);
      SQLException duplicate = new SQLException("duplicate key", "23505");
      SQLException timeout = new SQLException("lock timeout", "55P03");
      batch.setNextException(duplicateFirst ? duplicate : timeout);
      batch.setNextException(duplicateFirst ? timeout : duplicate);
      PersistenceException failure = new PersistenceException(batch);
      initFailingStore(failure);
      for (Pair<String, Executable> write : writes()) {
        Assertions.assertSame(
            failure,
            Assertions.assertThrows(PersistenceException.class, write.getRight()),
            write.getLeft());
      }
    }
  }

  private List<Pair<String, Executable>> writes() {
    TableEntity table = table();
    NameIdentifier tagIdent = NameIdentifier.of(METALAKE, "tag1");
    return List.of(
        Pair.of("put", () -> store.put(table, false)),
        Pair.of(
            "update",
            () ->
                store.update(
                    TABLE_IDENT, TableEntity.class, Entity.EntityType.TABLE, entity -> entity)),
        Pair.of("delete", () -> store.delete(TABLE_IDENT, Entity.EntityType.TABLE)),
        Pair.of(
            "deleteAndGet",
            () ->
                store.deleteAndGet(
                    TABLE_IDENT, Entity.EntityType.TABLE, TableEntity.class, entity -> {})),
        Pair.of(
            "insertRelation",
            () ->
                store.insertRelation(
                    SupportsRelationOperations.Type.OWNER_REL,
                    TABLE_IDENT,
                    Entity.EntityType.TABLE,
                    NameIdentifier.of(METALAKE, "user1"),
                    Entity.EntityType.USER,
                    true)),
        Pair.of(
            "batchInsertRelations",
            () ->
                store.batchInsertRelations(
                    SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
                    List.of(TABLE_IDENT),
                    Entity.EntityType.TABLE,
                    tagIdent,
                    Entity.EntityType.TAG,
                    false)),
        Pair.of(
            "updateEntityRelations",
            () ->
                store.updateEntityRelations(
                    SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
                    TABLE_IDENT,
                    Entity.EntityType.TABLE,
                    new NameIdentifier[] {tagIdent},
                    new NameIdentifier[0])),
        Pair.of(
            "updateEntityRelations(RelationUpdate)",
            () ->
                store.updateEntityRelations(
                    RelationUpdate.of(
                        SupportsRelationOperations.Type.POLICY_TAG_REL,
                        tagIdent,
                        Entity.EntityType.TAG,
                        new RelationEdgeTarget[] {
                          RelationEdgeTarget.of(
                              NameIdentifier.of(METALAKE, "policy1"),
                              Entity.EntityType.POLICY,
                              null)
                        },
                        new RelationEdgeTarget[0]))),
        Pair.of(
            "batchDelete",
            () -> store.batchDelete(List.of(Pair.of(TABLE_IDENT, Entity.EntityType.TABLE)), false)),
        Pair.of("batchPut", () -> store.batchPut(List.of(table), true)),
        Pair.of(
            "insertDomainWithBackingTag",
            () ->
                store.insertDomainWithBackingTag(domain(), TagValue.of("tag1", "v1"), state -> {})),
        Pair.of(
            "insertDomainWithNewBackingTag",
            () -> store.insertDomainWithNewBackingTag(domain(), backingTag(), state -> {})),
        Pair.of(
            "updateDomainBackingTagAndGet",
            () ->
                store.updateDomainBackingTagAndGet(
                    DOMAIN_IDENT, TagValue.noValue("tag1"), AUDIT_INFO, state -> {})),
        Pair.of(
            "setDomainParentAndGet",
            () ->
                store.setDomainParentAndGet(
                    DOMAIN_IDENT,
                    NameIdentifierUtil.ofDomain(METALAKE, "parent"),
                    AUDIT_INFO,
                    state -> {})),
        Pair.of(
            "removeDomainParentAndGet",
            () -> store.removeDomainParentAndGet(DOMAIN_IDENT, state -> {})),
        Pair.of(
            "updateDomainStatusAndGet",
            () ->
                store.updateDomainStatusAndGet(
                    DOMAIN_IDENT, Domain.Status.PUBLISHED, AUDIT_INFO, state -> {})));
  }

  private static DomainEntity domain() {
    return DomainEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(DOMAIN_IDENT.name())
        .withNamespace(DOMAIN_IDENT.namespace())
        .withProperties(ImmutableMap.of())
        .withStatus(Domain.Status.DRAFT)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private static TagEntity backingTag() {
    return TagEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName("tag1")
        .withNamespace(NamespaceUtil.ofTag(METALAKE))
        .withAllowedValues(new String[0])
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private List<TagEntity> associateTags(NameIdentifier[] tags) throws IOException {
    return store.updateEntityRelations(
        SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
        TABLE_IDENT,
        Entity.EntityType.TABLE,
        tags,
        new NameIdentifier[0]);
  }

  private TableEntity rename(TableEntity table) {
    return TableEntity.builder()
        .withId(table.id())
        .withName("t1_renamed")
        .withNamespace(table.namespace())
        .withColumns(table.columns())
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private TableEntity createTable() throws IOException {
    store.put(metalake(), false);
    store.put(catalog(), false);
    store.put(schema(), false);
    TableEntity table = table();
    store.put(table, false);
    return table;
  }

  /**
   * Runs a write while another session holds a row with {@code SELECT ... FOR UPDATE}, and checks
   * that the write fails as a conflict caused by H2's lock wait timeout.
   */
  private OptimisticLockException writeWhileRowHeld(
      String jdbcUrl, String table, String idColumn, long id, Executable write)
      throws SQLException {
    try (Connection holder = DriverManager.getConnection(jdbcUrl, USER, PASSWORD)) {
      holder.setAutoCommit(false);
      try {
        try (PreparedStatement statement =
            holder.prepareStatement(
                String.format(
                    "SELECT %s FROM %s WHERE %s = ? FOR UPDATE", idColumn, table, idColumn))) {
          statement.setLong(1, id);
          try (ResultSet rs = statement.executeQuery()) {
            Assertions.assertTrue(rs.next(), "the row must exist to be held");
          }
        }

        OptimisticLockException conflict =
            Assertions.assertThrows(OptimisticLockException.class, write);
        Assertions.assertTrue(
            conflict.getMessage().contains("lock wait timeout; retry the operation"),
            conflict.getMessage());
        SQLException timeout = findSqlException(conflict);
        Assertions.assertNotNull(timeout, "the lock wait timeout stays attached for diagnosis");
        Assertions.assertEquals(50200, timeout.getErrorCode());
        return conflict;
      } finally {
        holder.rollback();
      }
    }
  }

  /** Creates a store whose backend fails every call with the given failure. */
  private void initFailingStore(RuntimeException failure) throws IllegalAccessException {
    RelationalBackend backend =
        Mockito.mock(
            RelationalBackend.class,
            invocation -> {
              throw failure;
            });
    store = new RelationalEntityStore();
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", Mockito.mock(EntityCache.class), true);
  }

  private String initH2Store() throws IOException, IllegalAccessException {
    dbPath = Files.createTempDirectory("gravitino_lock_wait_timeout_").toString();
    String jdbcUrl = String.format("jdbc:h2:file:%s;MODE=MYSQL", new File(dbPath, "db").getPath());

    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.ENTITY_STORE)).thenReturn(Configs.RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_STORE))
        .thenReturn(Configs.DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL)).thenReturn(jdbcUrl);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn(USER);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn(PASSWORD);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER))
        .thenReturn("org.h2.Driver");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS))
        .thenReturn(Configs.DEFAULT_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_IDLE_CONNECTIONS))
        .thenReturn(10);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS))
        .thenReturn(Configs.DEFAULT_RELATIONAL_JDBC_BACKEND_MAX_WAIT_MILLISECONDS);
    Mockito.when(config.get(Configs.STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    Mockito.when(config.get(Configs.VERSION_RETENTION_COUNT)).thenReturn(1L);
    Mockito.when(config.get(Configs.ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS)).thenReturn(3L);
    Mockito.when(config.get(Configs.ENTITY_CHANGE_LOG_POLL_BATCH_SIZE))
        .thenReturn(Configs.DEFAULT_ENTITY_CHANGE_LOG_POLL_BATCH_SIZE);
    Mockito.when(config.get(Configs.ENTITY_CHANGE_LOG_REPLAY_WINDOW_SECS))
        .thenReturn(Configs.DEFAULT_ENTITY_CHANGE_LOG_REPLAY_WINDOW_SECS);
    Mockito.when(config.get(Configs.ENTITY_CHANGE_LOG_REPLAY_MAX_ENTRIES))
        .thenReturn(Configs.DEFAULT_ENTITY_CHANGE_LOG_REPLAY_MAX_ENTRIES);
    Mockito.when(config.get(Configs.ENTITY_CHANGE_LOG_RETENTION_SECS)).thenReturn(24 * 60 * 60L);
    Mockito.when(config.get(Configs.ENTITY_CHANGE_LOG_CLEANUP_INTERVAL_SECS)).thenReturn(60 * 60L);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(false);
    Mockito.when(config.get(Configs.SCHEMA_SEPARATOR)).thenReturn(":");

    previousConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    previousIdGenerator = FieldUtils.readField(GravitinoEnv.getInstance(), "idGenerator", true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "idGenerator", RandomIdGenerator.INSTANCE, true);

    store = new RelationalEntityStore();
    store.initialize(config);
    return jdbcUrl;
  }

  private static SQLException findSqlException(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof SQLException) {
        return (SQLException) cause;
      }
    }
    return null;
  }

  private static BaseMetalake metalake() {
    return BaseMetalake.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(METALAKE)
        .withAuditInfo(AUDIT_INFO)
        .withComment("")
        .withProperties(null)
        .withVersion(SchemaVersion.V_0_1)
        .build();
  }

  private static CatalogEntity catalog() {
    return CatalogEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(CATALOG)
        .withNamespace(Namespace.of(METALAKE))
        .withType(Catalog.Type.RELATIONAL)
        .withProvider("test")
        .withComment("")
        .withProperties(null)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private static SchemaEntity schema() {
    return SchemaEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(SCHEMA)
        .withNamespace(Namespace.of(METALAKE, CATALOG))
        .withComment("")
        .withProperties(null)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private static TableEntity table() {
    return TableEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(TABLE_IDENT.name())
        .withNamespace(TABLE_IDENT.namespace())
        .withAuditInfo(AUDIT_INFO)
        .build();
  }
}
