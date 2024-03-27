/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage;

import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_STORE;
import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.gravitino.Configs.KV_DELETE_AFTER_TIME;
import static com.datastrato.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static com.datastrato.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.EntityStoreFactory;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NonEmptyEntityException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.storage.relational.RelationalEntityStore;
import com.datastrato.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class TestEntityStorage {
  public static final String KV_STORE_PATH =
      "/tmp/gravitino_kv_entityStore_" + UUID.randomUUID().toString().replace("-", "");

  private static final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_entityStore_" + UUID.randomUUID().toString().replace("-", "");
  private static final String DB_DIR = JDBC_STORE_PATH + "/testdb";

  static Object[] storageProvider() {
    return new Object[] {Configs.DEFAULT_ENTITY_STORE, Configs.RELATIONAL_ENTITY_STORE};
  }

  private void init(String type, Config config) {
    Preconditions.checkArgument(StringUtils.isNotBlank(type));
    if (type.equals(Configs.DEFAULT_ENTITY_STORE)) {
      try {
        FileUtils.deleteDirectory(FileUtils.getFile(KV_STORE_PATH));
      } catch (Exception e) {
        // Ignore
      }
      Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
      Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
      Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
      Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(KV_STORE_PATH);

      Assertions.assertEquals(KV_STORE_PATH, config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH));
      Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
      Mockito.when(config.get(KV_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    } else if (type.equals(Configs.RELATIONAL_ENTITY_STORE)) {
      File dir = new File(DB_DIR);
      if (dir.exists() || !dir.isDirectory()) {
        dir.delete();
      }
      dir.mkdirs();
      Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
      Mockito.when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
          .thenReturn(String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", DB_DIR));
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("123");
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    } else {
      throw new UnsupportedOperationException("Unsupported entity store type: " + type);
    }
  }

  private void prepareJdbcTable() {
    // Read the ddl sql to create table
    String scriptPath = "h2/h2-init.sql";
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      StringBuilder ddlBuilder = new StringBuilder();
      IOUtils.readLines(
              Objects.requireNonNull(
                  this.getClass().getClassLoader().getResourceAsStream(scriptPath)),
              StandardCharsets.UTF_8)
          .forEach(line -> ddlBuilder.append(line).append("\n"));
      statement.execute(ddlBuilder.toString());
    } catch (Exception e) {
      throw new IllegalStateException("Create tables failed", e);
    }
  }

  private void destroy(String type) {
    Preconditions.checkArgument(StringUtils.isNotBlank(type));
    if (type.equals(Configs.DEFAULT_ENTITY_STORE)) {
      try {
        FileUtils.deleteDirectory(FileUtils.getFile(KV_STORE_PATH));
      } catch (Exception e) {
        // Ignore
      }
    } else if (type.equals(Configs.RELATIONAL_ENTITY_STORE)) {
      dropAllTables();
      File dir = new File(DB_DIR);
      if (dir.exists()) {
        dir.delete();
      }
    } else {
      throw new UnsupportedOperationException("Unsupported entity store type: " + type);
    }
  }

  private static void dropAllTables() {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection()) {
        try (Statement statement = connection.createStatement()) {
          String query = "SHOW TABLES";
          List<String> tableList = new ArrayList<>();
          try (ResultSet rs = statement.executeQuery(query)) {
            while (rs.next()) {
              tableList.add(rs.getString(1));
            }
          }
          for (String table : tableList) {
            statement.execute("DROP TABLE " + table);
          }
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Drop table failed", e);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testRestart(String type) throws IOException {
    Config config = Mockito.mock(Config.class);
    init(type, config);
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      if (store instanceof RelationalEntityStore) {
        prepareJdbcTable();
      }

      BaseMetalake metalake =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
      CatalogEntity catalog =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
      CatalogEntity catalogCopy =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake"),
              "catalogCopy",
              auditInfo);

      SchemaEntity schema1 =
          createSchemaEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog"),
              "schema1",
              auditInfo);
      TableEntity table1 =
          createTableEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema1"),
              "table1",
              auditInfo);
      FilesetEntity fileset1 =
          createFilesetEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema1"),
              "fileset1",
              auditInfo);

      // Store all entities
      store.put(metalake);
      store.put(catalog);
      store.put(catalogCopy);
      store.put(schema1);
      store.put(table1);
      store.put(fileset1);

      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake"), Entity.EntityType.METALAKE, BaseMetalake.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog"),
                  Entity.EntityType.CATALOG,
                  CatalogEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1"),
                  Entity.EntityType.SCHEMA,
                  SchemaEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "table1"),
                  Entity.EntityType.TABLE,
                  TableEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "fileset1"),
                  Entity.EntityType.FILESET,
                  FilesetEntity.class));
    }

    // It will automatically close the store we create before, then we reopen the entity store
    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);

      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake"), Entity.EntityType.METALAKE, BaseMetalake.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog"),
                  Entity.EntityType.CATALOG,
                  CatalogEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1"),
                  Entity.EntityType.SCHEMA,
                  SchemaEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "table1"),
                  Entity.EntityType.TABLE,
                  TableEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "fileset1"),
                  Entity.EntityType.FILESET,
                  FilesetEntity.class));
      destroy(type);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testEntityUpdate(String type) throws Exception {
    Config config = Mockito.mock(Config.class);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      if (store instanceof RelationalEntityStore) {
        prepareJdbcTable();
      }

      BaseMetalake metalake =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
      CatalogEntity catalog =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
      CatalogEntity catalogCopy =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake"),
              "catalogCopy",
              auditInfo);

      SchemaEntity schema1 =
          createSchemaEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog"),
              "schema1",
              auditInfo);
      TableEntity table1 =
          createTableEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema1"),
              "table1",
              auditInfo);
      FilesetEntity fileset1 =
          createFilesetEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema1"),
              "fileset1",
              auditInfo);

      SchemaEntity schema2 =
          createSchemaEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog"),
              "schema2",
              auditInfo);
      TableEntity table1InSchema2 =
          createTableEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema2"),
              "table1",
              auditInfo);
      FilesetEntity fileset1InSchema2 =
          createFilesetEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema2"),
              "fileset1",
              auditInfo);

      // Store all entities
      store.put(metalake);
      store.put(catalog);
      store.put(catalogCopy);
      store.put(schema1);
      store.put(schema2);
      store.put(table1);
      store.put(table1InSchema2);
      store.put(fileset1);
      store.put(fileset1InSchema2);

      validateMetalakeChanged(store, metalake);
      validateCatalogChanged(store, catalog);
      validateSchemaChanged(store, schema1);
      validateTableChanged(store, table1);
      validateFilesetChanged(store, fileset1);
      validateDeletedTable(store);
      validateDeletedFileset(store);
      validateAlreadyExistEntity(store, schema2);
      validateNotChangedEntity(store, schema2);

      destroy(type);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testEntityDelete(String type) throws IOException {
    // TODO
    Config config = Mockito.mock(Config.class);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      if (store instanceof RelationalEntityStore) {
        prepareJdbcTable();
      }

      BaseMetalake metalake = createBaseMakeLake(1L, "metalake", auditInfo);
      CatalogEntity catalog = createCatalog(1L, Namespace.of("metalake"), "catalog", auditInfo);
      CatalogEntity catalogCopy =
          createCatalog(2L, Namespace.of("metalake"), "catalogCopy", auditInfo);

      SchemaEntity schema1 =
          createSchemaEntity(1L, Namespace.of("metalake", "catalog"), "schema1", auditInfo);
      TableEntity table1 =
          createTableEntity(
              1L, Namespace.of("metalake", "catalog", "schema1"), "table1", auditInfo);
      FilesetEntity fileset1 =
          createFilesetEntity(
              1L, Namespace.of("metalake", "catalog", "schema1"), "fileset1", auditInfo);

      SchemaEntity schema2 =
          createSchemaEntity(2L, Namespace.of("metalake", "catalog"), "schema2", auditInfo);
      TableEntity table1InSchema2 =
          createTableEntity(
              2L, Namespace.of("metalake", "catalog", "schema2"), "table1", auditInfo);
      FilesetEntity fileset1InSchema2 =
          createFilesetEntity(
              2L, Namespace.of("metalake", "catalog", "schema2"), "fileset1", auditInfo);

      // Store all entities
      store.put(metalake);
      store.put(catalog);
      store.put(catalogCopy);
      store.put(schema1);
      store.put(schema2);
      store.put(table1);
      store.put(table1InSchema2);
      store.put(fileset1);
      store.put(fileset1InSchema2);

      validateAllEntityExist(
          metalake,
          store,
          catalog,
          catalogCopy,
          schema1,
          schema2,
          table1,
          table1InSchema2,
          fileset1,
          fileset1InSchema2);

      validateDeleteTable(store, schema2, table1, table1InSchema2);

      validateDeleteFileset(store, schema2, fileset1, fileset1InSchema2);

      validateDeleteSchema(store, schema1, table1, fileset1);

      validateDeleteCatalog(
          store, catalog, table1, schema1, table1InSchema2, schema2, fileset1, fileset1InSchema2);

      validateDeleteMetalake(store, metalake, catalogCopy);

      // Store all entities again
      BaseMetalake metalakeNew =
          createBaseMakeLake(
              RandomIdGenerator.INSTANCE.nextId(),
              metalake.name(),
              (AuditInfo) metalake.auditInfo());
      store.put(metalakeNew);
      CatalogEntity catalogNew =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              catalog.namespace(),
              catalog.name(),
              (AuditInfo) catalog.auditInfo());
      store.put(catalogNew);
      CatalogEntity catalogCopyNew =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              catalogCopy.namespace(),
              catalogCopy.name(),
              (AuditInfo) catalogCopy.auditInfo());
      store.put(catalogCopyNew);
      SchemaEntity schema1New =
          createSchemaEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              schema1.namespace(),
              schema1.name(),
              schema1.auditInfo());
      store.put(schema1New);
      SchemaEntity schema2New =
          createSchemaEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              schema2.namespace(),
              schema2.name(),
              schema2.auditInfo());
      store.put(schema2New);
      TableEntity table1New =
          createTableEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              table1.namespace(),
              table1.name(),
              table1.auditInfo());
      store.put(table1New);
      TableEntity table1InSchema2New =
          createTableEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              table1InSchema2.namespace(),
              table1InSchema2.name(),
              table1InSchema2.auditInfo());
      store.put(table1InSchema2New);
      FilesetEntity fileset1New =
          createFilesetEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              fileset1.namespace(),
              fileset1.name(),
              fileset1.auditInfo());
      store.put(fileset1New);
      FilesetEntity fileset1InSchema2New =
          createFilesetEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              fileset1InSchema2.namespace(),
              fileset1InSchema2.name(),
              fileset1InSchema2.auditInfo());
      store.put(fileset1InSchema2New);

      validateDeleteTableCascade(store, table1New);

      validateDeleteFilesetCascade(store, fileset1New);

      validateDeleteSchemaCascade(store, schema1New, table1New, fileset1New);

      validateDeleteCatalogCascade(store, catalogNew, schema2New);

      validateDeleteMetalakeCascade(store, metalakeNew, catalogNew, schema2New);

      destroy(type);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSameNameUnderANameSpace(String type) throws IOException {
    Config config = Mockito.mock(Config.class);
    init(type, config);
    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      if (store instanceof RelationalEntityStore) {
        prepareJdbcTable();
      }

      AuditInfo auditInfo =
          AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

      BaseMetalake metalake1 =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake1", auditInfo);
      CatalogEntity catalog1 =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake1"),
              "catalog1",
              auditInfo);
      SchemaEntity schema1 =
          createSchemaEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake1", "catalog1"),
              "schema1",
              auditInfo);

      Namespace namespace = Namespace.of("metalake1", "catalog1", "schema1");
      TableEntity table1 =
          createTableEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, "sameName", auditInfo);

      FilesetEntity filesetEntity1 =
          createFilesetEntity(
              RandomIdGenerator.INSTANCE.nextId(), namespace, "sameName", auditInfo);

      store.put(metalake1);
      store.put(catalog1);
      store.put(schema1);
      store.put(table1);
      store.put(filesetEntity1);

      NameIdentifier identifier = NameIdentifier.of("metalake1", "catalog1", "schema1", "sameName");

      TableEntity loadedTableEntity =
          store.get(identifier, Entity.EntityType.TABLE, TableEntity.class);
      Assertions.assertEquals(table1.id(), loadedTableEntity.id());
      FilesetEntity loadedFilesetEntity =
          store.get(identifier, Entity.EntityType.FILESET, FilesetEntity.class);
      Assertions.assertEquals(filesetEntity1.id(), loadedFilesetEntity.id());

      // Remove anyone will not affect another
      store.delete(identifier, Entity.EntityType.TABLE);
      store.get(identifier, Entity.EntityType.FILESET, FilesetEntity.class);

      // JDBC use id as the primary key, so we need to change the id of table1 if we want to store
      // it again
      table1 =
          createTableEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, "sameName", auditInfo);
      store.put(table1);
      store.delete(identifier, Entity.EntityType.FILESET);
      store.get(identifier, Entity.EntityType.TABLE, TableEntity.class);

      // Rename anyone will not affect another
      filesetEntity1 =
          createFilesetEntity(
              RandomIdGenerator.INSTANCE.nextId(), namespace, "sameName", auditInfo);
      store.put(filesetEntity1);
      long table1Id = table1.id();
      store.update(
          identifier,
          TableEntity.class,
          Entity.EntityType.TABLE,
          e -> createTableEntity(table1Id, namespace, "sameNameChanged", e.auditInfo()));

      NameIdentifier changedNameIdentifier =
          NameIdentifier.of("metalake1", "catalog1", "schema1", "sameNameChanged");
      store.get(changedNameIdentifier, Entity.EntityType.TABLE, TableEntity.class);
      store.get(identifier, Entity.EntityType.FILESET, FilesetEntity.class);

      table1 =
          createTableEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, "sameName", auditInfo);
      store.put(table1);
      long filesetId = filesetEntity1.id();
      store.update(
          identifier,
          FilesetEntity.class,
          Entity.EntityType.FILESET,
          e -> createFilesetEntity(filesetId, namespace, "sameNameChanged", e.auditInfo()));

      store.get(identifier, Entity.EntityType.TABLE, TableEntity.class);
      store.get(changedNameIdentifier, Entity.EntityType.FILESET, FilesetEntity.class);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testDeleteAndRename(String type) throws IOException {
    Config config = Mockito.mock(Config.class);
    init(type, config);
    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      if (store instanceof RelationalEntityStore) {
        prepareJdbcTable();
      }

      AuditInfo auditInfo =
          AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

      BaseMetalake metalake1 =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake1", auditInfo);
      BaseMetalake metalake2 =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake2", auditInfo);
      BaseMetalake metalake3 =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake3", auditInfo);

      store.put(metalake1);
      store.put(metalake2);
      store.put(metalake3);

      store.delete(NameIdentifier.of("metalake1"), Entity.EntityType.METALAKE);
      store.delete(NameIdentifier.of("metalake2"), Entity.EntityType.METALAKE);
      store.delete(NameIdentifier.of("metalake3"), Entity.EntityType.METALAKE);

      // Rename metalake1 --> metalake2
      BaseMetalake metalake1New =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake1", auditInfo);
      store.put(metalake1New);
      store.update(
          NameIdentifier.of("metalake1"),
          BaseMetalake.class,
          Entity.EntityType.METALAKE,
          e -> createBaseMakeLake(metalake1New.id(), "metalake2", (AuditInfo) e.auditInfo()));

      // Rename metalake3 --> metalake1
      BaseMetalake metalake3New1 =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake3", auditInfo);
      store.put(metalake3New1);
      store.update(
          NameIdentifier.of("metalake3"),
          BaseMetalake.class,
          Entity.EntityType.METALAKE,
          e -> createBaseMakeLake(metalake3New1.id(), "metalake1", (AuditInfo) e.auditInfo()));

      // Rename metalake3 --> metalake2
      BaseMetalake metalake3New2 =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake3", auditInfo);
      store.put(metalake3New2);
      Thread.sleep(1000);
      store.delete(NameIdentifier.of("metalake2"), Entity.EntityType.METALAKE);
      store.update(
          NameIdentifier.of("metalake3"),
          BaseMetalake.class,
          Entity.EntityType.METALAKE,
          e -> createBaseMakeLake(metalake3New2.id(), "metalake2", (AuditInfo) e.auditInfo()));

      // Finally, only metalake2 and metalake1 are left.
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake2"), Entity.EntityType.METALAKE, BaseMetalake.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake1"), Entity.EntityType.METALAKE, BaseMetalake.class));
      NameIdentifier id = NameIdentifier.of("metalake3");
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(id, Entity.EntityType.METALAKE, BaseMetalake.class));

      // Test catalog
      CatalogEntity catalog1 =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake1"),
              "catalog1",
              auditInfo);
      CatalogEntity catalog2 =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake1"),
              "catalog2",
              auditInfo);

      store.put(catalog1);
      store.put(catalog2);

      store.delete(NameIdentifier.of("metalake1", "catalog1"), Entity.EntityType.CATALOG);
      store.delete(NameIdentifier.of("metalake1", "catalog2"), Entity.EntityType.CATALOG);

      CatalogEntity catalog1New =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              catalog1.namespace(),
              catalog1.name(),
              (AuditInfo) catalog1.auditInfo());
      store.put(catalog1New);
      // Should be OK;
      store.update(
          catalog1New.nameIdentifier(),
          CatalogEntity.class,
          Entity.EntityType.CATALOG,
          e ->
              createCatalog(
                  catalog1New.id(),
                  Namespace.of("metalake1"),
                  "catalog2",
                  (AuditInfo) e.auditInfo()));

      // Test schema
      SchemaEntity schema1 =
          createSchemaEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake1", "catalog2"),
              "schema1",
              auditInfo);
      SchemaEntity schema2 =
          createSchemaEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake1", "catalog2"),
              "schema2",
              auditInfo);

      store.put(schema1);
      store.put(schema2);

      store.delete(NameIdentifier.of("metalake1", "catalog2", "schema1"), Entity.EntityType.SCHEMA);
      store.delete(NameIdentifier.of("metalake1", "catalog2", "schema2"), Entity.EntityType.SCHEMA);

      SchemaEntity schema1New =
          createSchemaEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              schema1.namespace(),
              schema1.name(),
              schema1.auditInfo());
      store.put(schema1New);
      store.update(
          schema1New.nameIdentifier(),
          SchemaEntity.class,
          Entity.EntityType.SCHEMA,
          e ->
              createSchemaEntity(
                  schema1New.id(),
                  Namespace.of("metalake1", "catalog2"),
                  "schema2",
                  e.auditInfo()));

      // Test table
      TableEntity table1 =
          createTableEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake1", "catalog2", "schema2"),
              "table1",
              auditInfo);
      TableEntity table2 =
          createTableEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake1", "catalog2", "schema2"),
              "table2",
              auditInfo);

      store.put(table1);
      store.put(table2);

      store.delete(
          NameIdentifier.of("metalake1", "catalog2", "schema2", "table1"), Entity.EntityType.TABLE);
      store.delete(
          NameIdentifier.of("metalake1", "catalog2", "schema2", "table2"), Entity.EntityType.TABLE);

      TableEntity table1New =
          createTableEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              table1.namespace(),
              table1.name(),
              table1.auditInfo());
      store.put(table1New);
      store.update(
          table1New.nameIdentifier(),
          TableEntity.class,
          Entity.EntityType.TABLE,
          e ->
              createTableEntity(
                  table1New.id(),
                  Namespace.of("metalake1", "catalog2", "schema2"),
                  "table2",
                  e.auditInfo()));

      // Test Fileset
      FilesetEntity fileset1 =
          createFilesetEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake1", "catalog2", "schema2"),
              "fileset1",
              auditInfo);
      FilesetEntity fileset2 =
          createFilesetEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake1", "catalog2", "schema2"),
              "fileset2",
              auditInfo);

      store.put(fileset1);
      store.put(fileset2);

      store.delete(
          NameIdentifier.of("metalake1", "catalog2", "schema2", "fileset1"),
          Entity.EntityType.FILESET);
      store.delete(
          NameIdentifier.of("metalake1", "catalog2", "schema2", "fileset2"),
          Entity.EntityType.FILESET);

      FilesetEntity fileset1New =
          createFilesetEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              fileset1.namespace(),
              fileset1.name(),
              fileset1.auditInfo());
      store.put(fileset1New);
      store.update(
          fileset1New.nameIdentifier(),
          FilesetEntity.class,
          Entity.EntityType.FILESET,
          e ->
              createFilesetEntity(
                  fileset1New.id(),
                  Namespace.of("metalake1", "catalog2", "schema2"),
                  "fileset2",
                  e.auditInfo()));

      destroy(type);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static BaseMetalake createBaseMakeLake(Long id, String name, AuditInfo auditInfo) {
    return BaseMetalake.builder()
        .withId(id)
        .withName(name)
        .withAuditInfo(auditInfo)
        .withComment("")
        .withProperties(null)
        .withVersion(SchemaVersion.V_0_1)
        .build();
  }

  public static CatalogEntity createCatalog(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return CatalogEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withType(Catalog.Type.RELATIONAL)
        .withProvider("test")
        .withComment("")
        .withProperties(null)
        .withAuditInfo(auditInfo)
        .build();
  }

  public static SchemaEntity createSchemaEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return SchemaEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment("")
        .withProperties(null)
        .withAuditInfo(auditInfo)
        .build();
  }

  public static TableEntity createTableEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return TableEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(auditInfo)
        .build();
  }

  public static FilesetEntity createFilesetEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return FilesetEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withFilesetType(Fileset.Type.MANAGED)
        .withStorageLocation("/tmp")
        .withComment("")
        .withProperties(null)
        .withAuditInfo(auditInfo)
        .build();
  }

  private void validateDeleteFilesetCascade(EntityStore store, FilesetEntity fileset1)
      throws IOException {
    // Delete the fileset 'metalake.catalog.schema1.fileset1'
    Assertions.assertTrue(store.delete(fileset1.nameIdentifier(), Entity.EntityType.FILESET, true));
    Assertions.assertFalse(store.exists(fileset1.nameIdentifier(), Entity.EntityType.FILESET));
  }

  private void validateDeleteTableCascade(EntityStore store, TableEntity table1)
      throws IOException {
    // Delete the table 'metalake.catalog.schema1.table1'
    Assertions.assertTrue(store.delete(table1.nameIdentifier(), Entity.EntityType.TABLE, true));
    Assertions.assertFalse(store.exists(table1.nameIdentifier(), Entity.EntityType.TABLE));
  }

  private void validateDeleteFileset(
      EntityStore store,
      SchemaEntity schema2,
      FilesetEntity fileset1,
      FilesetEntity fileset1InSchema2)
      throws IOException {
    // Delete the fileset 'metalake.catalog.schema2.fileset1'
    Assertions.assertTrue(
        store.delete(fileset1InSchema2.nameIdentifier(), Entity.EntityType.FILESET));
    Assertions.assertFalse(
        store.exists(fileset1InSchema2.nameIdentifier(), Entity.EntityType.FILESET));

    // Make sure fileset 'metalake.catalog.schema1.fileset1' still exist;
    Assertions.assertEquals(
        fileset1,
        store.get(fileset1.nameIdentifier(), Entity.EntityType.FILESET, FilesetEntity.class));
    // Make sure schema 'metalake.catalog.schema2' still exist;
    Assertions.assertEquals(
        schema2, store.get(schema2.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class));
  }

  private void validateDeleteMetalakeCascade(
      EntityStore store, BaseMetalake metalake, CatalogEntity catalog, SchemaEntity schema2)
      throws IOException {
    Assertions.assertTrue(
        store.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, true));

    // catalog has already deleted, so we can't delete it again and should return false
    Assertions.assertFalse(store.exists(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertFalse(store.exists(schema2.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(store.exists(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
  }

  private void validateDeleteCatalogCascade(
      EntityStore store, CatalogEntity catalog, SchemaEntity schema2) throws IOException {
    Assertions.assertThrowsExactly(
        NonEmptyEntityException.class,
        () -> store.delete(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    store.delete(catalog.nameIdentifier(), Entity.EntityType.CATALOG, true);
    NameIdentifier id = catalog.nameIdentifier();
    Assertions.assertThrowsExactly(
        NoSuchEntityException.class,
        () -> store.get(id, Entity.EntityType.CATALOG, CatalogEntity.class));

    Assertions.assertThrowsExactly(
        NoSuchEntityException.class,
        () -> store.get(schema2.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class));
  }

  private void validateDeleteSchemaCascade(
      EntityStore store, SchemaEntity schema1, TableEntity table1, FilesetEntity fileset1)
      throws IOException {
    TableEntity table1New =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            table1.namespace(),
            table1.name(),
            table1.auditInfo());
    store.put(table1New);
    FilesetEntity fileset1New =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            fileset1.namespace(),
            fileset1.name(),
            fileset1.auditInfo());
    store.put(fileset1New);

    Assertions.assertThrowsExactly(
        NonEmptyEntityException.class,
        () -> store.delete(schema1.nameIdentifier(), Entity.EntityType.SCHEMA));

    Assertions.assertEquals(
        schema1, store.get(schema1.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class));

    // Test cascade delete
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    store.delete(schema1.nameIdentifier(), Entity.EntityType.SCHEMA, true);
    try {
      store.get(table1.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class);
    } catch (Exception e) {
      Assertions.assertTrue(e instanceof NoSuchEntityException);
      Assertions.assertTrue(e.getMessage().contains("schema1"));
    }

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> store.get(fileset1.nameIdentifier(), Entity.EntityType.FILESET, FilesetEntity.class));
  }

  private static void validateDeleteMetalake(
      EntityStore store, BaseMetalake metalake, CatalogEntity catalogCopy) throws IOException {
    // Now delete catalog 'catalogCopy' and metalake
    Assertions.assertThrowsExactly(
        NonEmptyEntityException.class,
        () -> store.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
    store.delete(catalogCopy.nameIdentifier(), Entity.EntityType.CATALOG);
    Assertions.assertFalse(store.exists(catalogCopy.nameIdentifier(), Entity.EntityType.CATALOG));

    store.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE);
    Assertions.assertFalse(store.exists(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
  }

  private static void validateDeleteCatalog(
      EntityStore store,
      CatalogEntity catalog,
      TableEntity table1,
      SchemaEntity schema1,
      TableEntity table1InSchema2,
      SchemaEntity schema2,
      FilesetEntity fileset1,
      FilesetEntity fileset1InSchema2)
      throws IOException {
    // Now try to delete all schemas under catalog;
    Assertions.assertThrowsExactly(
        NonEmptyEntityException.class,
        () -> store.delete(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    store.delete(table1.nameIdentifier(), Entity.EntityType.TABLE);
    store.delete(fileset1.nameIdentifier(), Entity.EntityType.FILESET);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    store.delete(schema1.nameIdentifier(), Entity.EntityType.SCHEMA);
    store.delete(table1InSchema2.nameIdentifier(), Entity.EntityType.TABLE);
    Assertions.assertFalse(
        store.exists(fileset1InSchema2.nameIdentifier(), Entity.EntityType.FILESET));
    store.delete(schema2.nameIdentifier(), Entity.EntityType.SCHEMA);

    store.delete(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    Assertions.assertFalse(store.exists(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
  }

  private static void validateDeleteSchema(
      EntityStore store, SchemaEntity schema1, TableEntity table1, FilesetEntity fileset1)
      throws IOException {
    // Delete the schema 'metalake.catalog.schema1' but failed, because it ha sub-entities;
    NonEmptyEntityException exception =
        Assertions.assertThrowsExactly(
            NonEmptyEntityException.class,
            () -> store.delete(schema1.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(exception.getMessage().contains("metalake.catalog.schema1"));
    // Make sure schema 'metalake.catalog.schema1' and table 'metalake.catalog.schema1.table1'
    // and table 'metalake.catalog.schema1.fileset1' has not been deleted yet;
    Assertions.assertTrue(store.exists(schema1.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(store.exists(table1.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertTrue(store.exists(fileset1.nameIdentifier(), Entity.EntityType.FILESET));

    // Delete table1,fileset1 and schema1
    Assertions.assertTrue(store.delete(table1.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertTrue(store.delete(fileset1.nameIdentifier(), Entity.EntityType.FILESET));
    Assertions.assertTrue(store.delete(schema1.nameIdentifier(), Entity.EntityType.SCHEMA));
    // Make sure table1, fileset1 in 'metalake.catalog.schema1' can't be access;
    Assertions.assertFalse(store.exists(table1.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertFalse(store.exists(fileset1.nameIdentifier(), Entity.EntityType.FILESET));
    Assertions.assertFalse(store.exists(schema1.nameIdentifier(), Entity.EntityType.SCHEMA));
    // Now we re-insert table1 and schema1, and everything should be OK
    SchemaEntity schema1New =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            schema1.namespace(),
            schema1.name(),
            schema1.auditInfo());
    store.put(schema1New);
    TableEntity table1New =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            table1.namespace(),
            table1.name(),
            table1.auditInfo());
    store.put(table1New);
    FilesetEntity fileset1New =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            fileset1.namespace(),
            fileset1.name(),
            fileset1.auditInfo());
    store.put(fileset1New);
    Assertions.assertEquals(
        schema1New,
        store.get(schema1.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class));
    Assertions.assertEquals(
        table1New, store.get(table1.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class));
    Assertions.assertEquals(
        fileset1New,
        store.get(fileset1.nameIdentifier(), Entity.EntityType.FILESET, FilesetEntity.class));
  }

  private void validateDeleteTable(
      EntityStore store, SchemaEntity schema2, TableEntity table1, TableEntity table1InSchema2)
      throws IOException {
    // Delete the table 'metalake.catalog.schema2.table1'
    Assertions.assertTrue(store.delete(table1InSchema2.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertFalse(store.exists(table1InSchema2.nameIdentifier(), Entity.EntityType.TABLE));

    // Make sure table 'metalake.catalog.schema1.table1' still exist;
    Assertions.assertEquals(
        table1, store.get(table1.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class));
    // Make sure schema 'metalake.catalog.schema2' still exist;
    Assertions.assertEquals(
        schema2, store.get(schema2.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class));
    // Re-insert table1Inschema2 and everything is OK
    TableEntity table1InSchema2New =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            table1InSchema2.namespace(),
            table1InSchema2.name(),
            table1InSchema2.auditInfo());
    store.put(table1InSchema2New);
    Assertions.assertTrue(store.exists(table1InSchema2.nameIdentifier(), Entity.EntityType.TABLE));
  }

  private static void validateAllEntityExist(
      BaseMetalake metalake,
      EntityStore store,
      CatalogEntity catalog,
      CatalogEntity catalogCopy,
      SchemaEntity schema1,
      SchemaEntity schema2,
      TableEntity table1,
      TableEntity table1InSchema2,
      FilesetEntity fileset1,
      FilesetEntity fileset1Inschema2)
      throws IOException {
    // Now try to get
    Assertions.assertEquals(
        metalake,
        store.get(metalake.nameIdentifier(), Entity.EntityType.METALAKE, BaseMetalake.class));
    Assertions.assertEquals(
        catalog,
        store.get(catalog.nameIdentifier(), Entity.EntityType.CATALOG, CatalogEntity.class));
    Assertions.assertEquals(
        catalogCopy,
        store.get(catalogCopy.nameIdentifier(), Entity.EntityType.CATALOG, CatalogEntity.class));
    Assertions.assertEquals(
        schema1, store.get(schema1.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class));
    Assertions.assertEquals(
        schema2, store.get(schema2.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class));
    Assertions.assertEquals(
        table1, store.get(table1.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class));
    Assertions.assertEquals(
        table1InSchema2,
        store.get(table1InSchema2.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class));
    Assertions.assertEquals(
        fileset1,
        store.get(fileset1.nameIdentifier(), Entity.EntityType.FILESET, FilesetEntity.class));
    Assertions.assertEquals(
        fileset1Inschema2,
        store.get(
            fileset1Inschema2.nameIdentifier(), Entity.EntityType.FILESET, FilesetEntity.class));
  }

  private void validateDeletedFileset(EntityStore store) throws IOException {
    store.delete(
        NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "fileset1"),
        Entity.EntityType.FILESET);
    // Update a deleted entities
    Assertions.assertThrowsExactly(
        NoSuchEntityException.class,
        () ->
            store.update(
                NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "fileset1"),
                FilesetEntity.class,
                Entity.EntityType.FILESET,
                (e) -> e));
  }

  private void validateFilesetChanged(EntityStore store, FilesetEntity filesetEntity)
      throws IOException {
    // Check fileset entities
    store.update(
        NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged", "fileset1"),
        FilesetEntity.class,
        Entity.EntityType.FILESET,
        e -> {
          AuditInfo auditInfo1 =
              AuditInfo.builder().withCreator("creator5").withCreateTime(Instant.now()).build();
          return createFilesetEntity(
              filesetEntity.id(),
              Namespace.of("metalakeChanged", "catalogChanged", "schemaChanged"),
              "filesetChanged",
              auditInfo1);
        });

    Assertions.assertThrowsExactly(
        NoSuchEntityException.class,
        () ->
            store.get(
                NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1", "fileset1"),
                Entity.EntityType.FILESET,
                FilesetEntity.class));
    FilesetEntity updatedFileset =
        store.get(
            NameIdentifier.of(
                "metalakeChanged", "catalogChanged", "schemaChanged", "filesetChanged"),
            Entity.EntityType.FILESET,
            FilesetEntity.class);
    Assertions.assertEquals("creator5", updatedFileset.auditInfo().creator());

    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "fileset1"),
            Entity.EntityType.FILESET,
            FilesetEntity.class));
  }

  private void validateNotChangedEntity(EntityStore store, SchemaEntity schema) throws IOException {
    // Update operations do not contain any changes in name
    store.update(
        NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2"),
        SchemaEntity.class,
        Entity.EntityType.SCHEMA,
        e -> {
          AuditInfo auditInfo1 =
              AuditInfo.builder().withCreator("creator6").withCreateTime(Instant.now()).build();
          return createSchemaEntity(
              schema.id(),
              Namespace.of("metalakeChanged", "catalogChanged"),
              "schema2",
              auditInfo1);
        });
    Assertions.assertEquals(
        "creator6",
        store
            .get(
                NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2"),
                Entity.EntityType.SCHEMA,
                SchemaEntity.class)
            .auditInfo()
            .creator());
  }

  private void validateAlreadyExistEntity(EntityStore store, SchemaEntity schema) {
    // The updated entities already existed, should throw exception
    Assertions.assertThrowsExactly(
        AlreadyExistsException.class,
        () ->
            store.update(
                NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2"),
                SchemaEntity.class,
                Entity.EntityType.SCHEMA,
                e -> {
                  AuditInfo auditInfo1 =
                      AuditInfo.builder()
                          .withCreator("creator5")
                          .withCreateTime(Instant.now())
                          .build();
                  return createSchemaEntity(
                      schema.id(),
                      Namespace.of("metalakeChanged", "catalogChanged"),
                      "schemaChanged",
                      auditInfo1);
                }));
  }

  private void validateMetalakeChanged(EntityStore store, BaseMetalake metalake)
      throws IOException {
    // Try to check an update option is what we expected
    store.update(
        metalake.nameIdentifier(),
        BaseMetalake.class,
        Entity.EntityType.METALAKE,
        e -> {
          AuditInfo auditInfo1 =
              AuditInfo.builder().withCreator("creator1").withCreateTime(Instant.now()).build();
          return createBaseMakeLake(metalake.id(), "metalakeChanged", auditInfo1);
        });

    // Check metalake entity and sub-entities are already changed.
    BaseMetalake updatedMetalake =
        store.get(
            NameIdentifier.of("metalakeChanged"), Entity.EntityType.METALAKE, BaseMetalake.class);
    Assertions.assertEquals("creator1", updatedMetalake.auditInfo().creator());

    Assertions.assertThrowsExactly(
        NoSuchEntityException.class,
        () ->
            store.get(
                NameIdentifier.of("metalake"), Entity.EntityType.METALAKE, BaseMetalake.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalog"),
            Entity.EntityType.CATALOG,
            CatalogEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalog", "schema1"),
            Entity.EntityType.SCHEMA,
            SchemaEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalog", "schema1", "table1"),
            Entity.EntityType.TABLE,
            TableEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalog", "schema1", "fileset1"),
            Entity.EntityType.FILESET,
            FilesetEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalog", "schema2"),
            Entity.EntityType.SCHEMA,
            SchemaEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalog", "schema2", "table1"),
            Entity.EntityType.TABLE,
            TableEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalog", "schema2", "fileset1"),
            Entity.EntityType.FILESET,
            FilesetEntity.class));
  }

  private void validateCatalogChanged(EntityStore store, CatalogEntity catalog) throws IOException {
    // Check catalog entities and sub-entities are already changed.
    store.update(
        NameIdentifier.of("metalakeChanged", "catalog"),
        CatalogEntity.class,
        Entity.EntityType.CATALOG,
        e -> {
          AuditInfo auditInfo1 =
              AuditInfo.builder().withCreator("creator2").withCreateTime(Instant.now()).build();
          return createCatalog(
              catalog.id(), Namespace.of("metalakeChanged"), "catalogChanged", auditInfo1);
        });
    CatalogEntity updatedCatalog =
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged"),
            Entity.EntityType.CATALOG,
            CatalogEntity.class);
    Assertions.assertEquals("creator2", updatedCatalog.auditInfo().creator());
    Assertions.assertThrowsExactly(
        NoSuchEntityException.class,
        () ->
            store.get(
                NameIdentifier.of("metalakeChanged", "catalog"),
                Entity.EntityType.CATALOG,
                CatalogEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1"),
            Entity.EntityType.SCHEMA,
            SchemaEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1", "table1"),
            Entity.EntityType.TABLE,
            TableEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1", "fileset1"),
            Entity.EntityType.FILESET,
            FilesetEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2"),
            Entity.EntityType.SCHEMA,
            SchemaEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "table1"),
            Entity.EntityType.TABLE,
            TableEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "fileset1"),
            Entity.EntityType.FILESET,
            FilesetEntity.class));
  }

  private void validateSchemaChanged(EntityStore store, SchemaEntity schema) throws IOException {
    // Check schema entities and sub-entities are already changed.
    store.update(
        NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1"),
        SchemaEntity.class,
        Entity.EntityType.SCHEMA,
        e -> {
          AuditInfo auditInfo1 =
              AuditInfo.builder().withCreator("creator3").withCreateTime(Instant.now()).build();
          return createSchemaEntity(
              schema.id(),
              Namespace.of("metalakeChanged", "catalogChanged"),
              "schemaChanged",
              auditInfo1);
        });

    Assertions.assertThrowsExactly(
        NoSuchEntityException.class,
        () ->
            store.get(
                NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1"),
                Entity.EntityType.SCHEMA,
                SchemaEntity.class));
    SchemaEntity updatedSchema =
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged"),
            Entity.EntityType.SCHEMA,
            SchemaEntity.class);
    Assertions.assertEquals("creator3", updatedSchema.auditInfo().creator());

    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged"),
            Entity.EntityType.SCHEMA,
            SchemaEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged", "table1"),
            Entity.EntityType.TABLE,
            TableEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged", "fileset1"),
            Entity.EntityType.FILESET,
            FilesetEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2"),
            Entity.EntityType.SCHEMA,
            SchemaEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "table1"),
            Entity.EntityType.TABLE,
            TableEntity.class));
    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "fileset1"),
            Entity.EntityType.FILESET,
            FilesetEntity.class));
  }

  private void validateTableChanged(EntityStore store, TableEntity table) throws IOException {
    // Check table entities
    store.update(
        NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged", "table1"),
        TableEntity.class,
        Entity.EntityType.TABLE,
        e -> {
          AuditInfo auditInfo1 =
              AuditInfo.builder().withCreator("creator4").withCreateTime(Instant.now()).build();
          return createTableEntity(
              table.id(),
              Namespace.of("metalakeChanged", "catalogChanged", "schemaChanged"),
              "tableChanged",
              auditInfo1);
        });

    Assertions.assertThrowsExactly(
        NoSuchEntityException.class,
        () ->
            store.get(
                NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1", "table1"),
                Entity.EntityType.TABLE,
                TableEntity.class));
    TableEntity updatedTable =
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged", "tableChanged"),
            Entity.EntityType.TABLE,
            TableEntity.class);
    Assertions.assertEquals("creator4", updatedTable.auditInfo().creator());

    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "table1"),
            Entity.EntityType.TABLE,
            TableEntity.class));
  }

  private void validateDeletedTable(EntityStore store) throws IOException {
    store.delete(
        NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "table1"),
        Entity.EntityType.TABLE);
    // Update a deleted entities
    Assertions.assertThrowsExactly(
        NoSuchEntityException.class,
        () ->
            store.update(
                NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "table1"),
                TableEntity.class,
                Entity.EntityType.TABLE,
                (e) -> e));
  }
}
