/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational;

import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_STORE;
import static com.datastrato.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.meta.TopicEntity;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import com.google.common.collect.ImmutableMap;
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
import org.apache.commons.io.IOUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestJDBCBackend {
  private static final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_entityStore_" + UUID.randomUUID().toString().replace("-", "");
  private static final String DB_DIR = JDBC_STORE_PATH + "/testdb";
  private static final Config config = Mockito.mock(Config.class);
  public static final ImmutableMap<String, String> RELATIONAL_BACKENDS =
      ImmutableMap.of(
          Configs.DEFAULT_ENTITY_RELATIONAL_STORE, JDBCBackend.class.getCanonicalName());
  public static RelationalBackend backend;

  @BeforeAll
  public static void setup() {
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

    String backendName = config.get(ENTITY_RELATIONAL_STORE);
    String className =
        RELATIONAL_BACKENDS.getOrDefault(backendName, Configs.DEFAULT_ENTITY_RELATIONAL_STORE);

    try {
      backend = (RelationalBackend) Class.forName(className).getDeclaredConstructor().newInstance();
      backend.initialize(config);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create and initialize RelationalBackend by name: " + backendName, e);
    }

    prepareJdbcTable();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    dropAllTables();
    File dir = new File(DB_DIR);
    if (dir.exists()) {
      dir.delete();
    }
    backend.close();
  }

  @BeforeEach
  public void init() {
    truncateAllTables();
  }

  private static void prepareJdbcTable() {
    // Read the ddl sql to create table
    String scriptPath = "h2/schema-h2.sql";
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      StringBuilder ddlBuilder = new StringBuilder();
      IOUtils.readLines(
              Objects.requireNonNull(
                  TestJDBCBackend.class.getClassLoader().getResourceAsStream(scriptPath)),
              StandardCharsets.UTF_8)
          .forEach(line -> ddlBuilder.append(line).append("\n"));
      statement.execute(ddlBuilder.toString());
    } catch (Exception e) {
      throw new IllegalStateException("Create tables failed", e);
    }
  }

  private static void truncateAllTables() {
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
            statement.execute("TRUNCATE TABLE " + table);
          }
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Truncate table failed", e);
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

  @Test
  public void testInsertAlreadyExistsException() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
    BaseMetalake metalakeCopy =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
    backend.insert(metalake, false);
    assertThrows(AlreadyExistsException.class, () -> backend.insert(metalakeCopy, false));

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofCatalog("metalake"),
            "catalog",
            auditInfo);
    CatalogEntity catalogCopy =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofCatalog("metalake"),
            "catalog",
            auditInfo);
    backend.insert(catalog, false);
    assertThrows(AlreadyExistsException.class, () -> backend.insert(catalogCopy, false));

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofSchema("metalake", "catalog"),
            "schema",
            auditInfo);
    SchemaEntity schemaCopy =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofSchema("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);
    assertThrows(AlreadyExistsException.class, () -> backend.insert(schemaCopy, false));

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofTable("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    TableEntity tableCopy =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofTable("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);
    assertThrows(AlreadyExistsException.class, () -> backend.insert(tableCopy, false));

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofFileset("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    FilesetEntity filesetCopy =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofFileset("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    backend.insert(fileset, false);
    assertThrows(AlreadyExistsException.class, () -> backend.insert(filesetCopy, false));

    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofFileset("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    TopicEntity topicCopy =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofFileset("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    backend.insert(topic, false);
    assertThrows(AlreadyExistsException.class, () -> backend.insert(topicCopy, false));
  }

  @Test
  public void testUpdateAlreadyExistsException() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
    BaseMetalake metalakeCopy =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake1", auditInfo);
    backend.insert(metalake, false);
    backend.insert(metalakeCopy, false);
    assertThrows(
        AlreadyExistsException.class,
        () ->
            backend.update(
                metalakeCopy.nameIdentifier(),
                Entity.EntityType.METALAKE,
                e -> createBaseMakeLake(metalakeCopy.id(), "metalake", auditInfo)));

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofCatalog("metalake"),
            "catalog",
            auditInfo);
    CatalogEntity catalogCopy =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofCatalog("metalake"),
            "catalog1",
            auditInfo);
    backend.insert(catalog, false);
    backend.insert(catalogCopy, false);
    assertThrows(
        AlreadyExistsException.class,
        () ->
            backend.update(
                catalogCopy.nameIdentifier(),
                Entity.EntityType.CATALOG,
                e ->
                    createCatalog(
                        catalogCopy.id(), catalogCopy.namespace(), "catalog", auditInfo)));

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofSchema("metalake", "catalog"),
            "schema",
            auditInfo);
    SchemaEntity schemaCopy =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofSchema("metalake", "catalog"),
            "schema1",
            auditInfo);
    backend.insert(schema, false);
    backend.insert(schemaCopy, false);
    assertThrows(
        AlreadyExistsException.class,
        () ->
            backend.update(
                schemaCopy.nameIdentifier(),
                Entity.EntityType.SCHEMA,
                e ->
                    createSchemaEntity(
                        schemaCopy.id(), schemaCopy.namespace(), "schema", auditInfo)));

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofTable("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    TableEntity tableCopy =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofTable("metalake", "catalog", "schema"),
            "table1",
            auditInfo);
    backend.insert(table, false);
    backend.insert(tableCopy, false);
    assertThrows(
        AlreadyExistsException.class,
        () ->
            backend.update(
                tableCopy.nameIdentifier(),
                Entity.EntityType.TABLE,
                e -> createTableEntity(tableCopy.id(), tableCopy.namespace(), "table", auditInfo)));

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofFileset("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    FilesetEntity filesetCopy =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofFileset("metalake", "catalog", "schema"),
            "fileset1",
            auditInfo);
    backend.insert(fileset, false);
    backend.insert(filesetCopy, false);
    assertThrows(
        AlreadyExistsException.class,
        () ->
            backend.update(
                filesetCopy.nameIdentifier(),
                Entity.EntityType.FILESET,
                e ->
                    createFilesetEntity(
                        filesetCopy.id(), filesetCopy.namespace(), "fileset", auditInfo)));

    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofFileset("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    TopicEntity topicCopy =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.ofFileset("metalake", "catalog", "schema"),
            "topic1",
            auditInfo);
    backend.insert(topic, false);
    backend.insert(topicCopy, false);
    assertThrows(
        AlreadyExistsException.class,
        () ->
            backend.update(
                topicCopy.nameIdentifier(),
                Entity.EntityType.TOPIC,
                e -> createTopicEntity(topicCopy.id(), topicCopy.namespace(), "topic", auditInfo)));
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

  public static TopicEntity createTopicEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return TopicEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment("test comment")
        .withProperties(ImmutableMap.of("key", "value"))
        .withAuditInfo(auditInfo)
        .build();
  }
}
