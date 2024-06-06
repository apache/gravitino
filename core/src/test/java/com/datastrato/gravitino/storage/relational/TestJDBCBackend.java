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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.authorization.AuthorizationUtils;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.GroupEntity;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.meta.TopicEntity;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.relational.mapper.GroupMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.UserMetaMapper;
import com.datastrato.gravitino.storage.relational.service.RoleMetaService;
import com.datastrato.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.datastrato.gravitino.utils.NamespaceUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            NamespaceUtil.ofCatalog("metalake"),
            "catalog",
            auditInfo);
    CatalogEntity catalogCopy =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog("metalake"),
            "catalog",
            auditInfo);
    backend.insert(catalog, false);
    assertThrows(AlreadyExistsException.class, () -> backend.insert(catalogCopy, false));

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema("metalake", "catalog"),
            "schema",
            auditInfo);
    SchemaEntity schemaCopy =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);
    assertThrows(AlreadyExistsException.class, () -> backend.insert(schemaCopy, false));

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    TableEntity tableCopy =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);
    assertThrows(AlreadyExistsException.class, () -> backend.insert(tableCopy, false));

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    FilesetEntity filesetCopy =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    backend.insert(fileset, false);
    assertThrows(AlreadyExistsException.class, () -> backend.insert(filesetCopy, false));

    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    TopicEntity topicCopy =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset("metalake", "catalog", "schema"),
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
            NamespaceUtil.ofCatalog("metalake"),
            "catalog",
            auditInfo);
    CatalogEntity catalogCopy =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog("metalake"),
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
            NamespaceUtil.ofSchema("metalake", "catalog"),
            "schema",
            auditInfo);
    SchemaEntity schemaCopy =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema("metalake", "catalog"),
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
            NamespaceUtil.ofTable("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    TableEntity tableCopy =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable("metalake", "catalog", "schema"),
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
            NamespaceUtil.ofFileset("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    FilesetEntity filesetCopy =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset("metalake", "catalog", "schema"),
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
            NamespaceUtil.ofFileset("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    TopicEntity topicCopy =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset("metalake", "catalog", "schema"),
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

  @Test
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    // meta data creation
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog("metalake"),
            "catalog",
            auditInfo);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    backend.insert(fileset, false);

    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    backend.insert(topic, false);

    // update fileset properties and version
    FilesetEntity filesetV2 =
        createFilesetEntity(
            fileset.id(),
            NamespaceUtil.ofFileset("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    filesetV2.properties().put("version", "2");
    backend.update(fileset.nameIdentifier(), Entity.EntityType.FILESET, e -> filesetV2);

    RoleEntity role =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace("metalake"),
            "role",
            auditInfo,
            "catalog");
    backend.insert(role, false);

    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace("metalake"),
            "user",
            auditInfo,
            Lists.newArrayList(role.name()),
            Lists.newArrayList(role.id()));
    backend.insert(user, false);

    GroupEntity group =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace("metalake"),
            "group",
            auditInfo,
            Lists.newArrayList(role.name()),
            Lists.newArrayList(role.id()));
    backend.insert(group, false);

    // another meta data creation
    BaseMetalake anotherMetaLake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "another-metalake", auditInfo);
    backend.insert(anotherMetaLake, false);

    CatalogEntity anotherCatalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog("another-metalake"),
            "another-catalog",
            auditInfo);
    backend.insert(anotherCatalog, false);

    SchemaEntity anotherSchema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema("another-metalake", "another-catalog"),
            "another-schema",
            auditInfo);
    backend.insert(anotherSchema, false);

    FilesetEntity anotherFileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset("another-metalake", "another-catalog", "another-schema"),
            "anotherFileset",
            auditInfo);
    backend.insert(anotherFileset, false);

    FilesetEntity anotherFilesetV2 =
        createFilesetEntity(
            anotherFileset.id(),
            NamespaceUtil.ofFileset("another-metalake", "another-catalog", "another-schema"),
            "anotherFileset",
            auditInfo);
    anotherFilesetV2.properties().put("version", "2");
    backend.update(
        anotherFileset.nameIdentifier(), Entity.EntityType.FILESET, e -> anotherFilesetV2);

    FilesetEntity anotherFilesetV3 =
        createFilesetEntity(
            anotherFileset.id(),
            NamespaceUtil.ofFileset("another-metalake", "another-catalog", "another-schema"),
            "anotherFileset",
            auditInfo);
    anotherFilesetV3.properties().put("version", "3");
    backend.update(
        anotherFileset.nameIdentifier(), Entity.EntityType.FILESET, e -> anotherFilesetV3);

    RoleEntity anotherRole =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace("another-metalake"),
            "another-role",
            auditInfo,
            "another-catalog");
    backend.insert(anotherRole, false);

    UserEntity anotherUser =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace("another-metalake"),
            "another-user",
            auditInfo,
            Lists.newArrayList(anotherRole.name()),
            Lists.newArrayList(anotherRole.id()));
    backend.insert(anotherUser, false);

    GroupEntity anotherGroup =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace("another-metalake"),
            "another-group",
            auditInfo,
            Lists.newArrayList(anotherRole.name()),
            Lists.newArrayList(anotherRole.id()));
    backend.insert(anotherGroup, false);

    // meta data list
    List<BaseMetalake> metaLakes = backend.list(metalake.namespace(), Entity.EntityType.METALAKE);
    assertTrue(metaLakes.contains(metalake));

    List<CatalogEntity> catalogs = backend.list(catalog.namespace(), Entity.EntityType.CATALOG);
    assertTrue(catalogs.contains(catalog));

    List<SchemaEntity> schemas = backend.list(schema.namespace(), Entity.EntityType.SCHEMA);
    assertTrue(schemas.contains(schema));

    List<TableEntity> tables = backend.list(table.namespace(), Entity.EntityType.TABLE);
    assertTrue(tables.contains(table));

    List<FilesetEntity> filesets = backend.list(fileset.namespace(), Entity.EntityType.FILESET);
    assertFalse(filesets.contains(fileset));
    assertTrue(filesets.contains(filesetV2));
    assertEquals("2", filesets.get(filesets.indexOf(filesetV2)).properties().get("version"));

    List<TopicEntity> topics = backend.list(topic.namespace(), Entity.EntityType.TOPIC);
    assertTrue(topics.contains(topic));

    RoleEntity roleEntity = backend.get(role.nameIdentifier(), Entity.EntityType.ROLE);
    assertEquals(role, roleEntity);
    assertEquals(1, RoleMetaService.getInstance().listRolesByUserId(user.id()).size());
    assertEquals(1, RoleMetaService.getInstance().listRolesByGroupId(group.id()).size());

    UserEntity userEntity = backend.get(user.nameIdentifier(), Entity.EntityType.USER);
    assertEquals(user, userEntity);
    assertEquals(
        1,
        SessionUtils.doWithCommitAndFetchResult(
                UserMetaMapper.class, mapper -> mapper.listUsersByRoleId(role.id()))
            .size());

    GroupEntity groupEntity = backend.get(group.nameIdentifier(), Entity.EntityType.GROUP);
    assertEquals(group, groupEntity);
    assertEquals(
        1,
        SessionUtils.doWithCommitAndFetchResult(
                GroupMetaMapper.class, mapper -> mapper.listGroupsByRoleId(role.id()))
            .size());

    // meta data soft delete
    backend.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, true);

    // check existence after soft delete
    assertFalse(backend.exists(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
    assertTrue(backend.exists(anotherMetaLake.nameIdentifier(), Entity.EntityType.METALAKE));

    assertFalse(backend.exists(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    assertTrue(backend.exists(anotherCatalog.nameIdentifier(), Entity.EntityType.CATALOG));

    assertFalse(backend.exists(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
    assertTrue(backend.exists(anotherSchema.nameIdentifier(), Entity.EntityType.SCHEMA));

    assertFalse(backend.exists(fileset.nameIdentifier(), Entity.EntityType.FILESET));
    assertTrue(backend.exists(anotherFileset.nameIdentifier(), Entity.EntityType.FILESET));

    assertFalse(backend.exists(table.nameIdentifier(), Entity.EntityType.TABLE));
    assertFalse(backend.exists(topic.nameIdentifier(), Entity.EntityType.TOPIC));

    assertFalse(backend.exists(role.nameIdentifier(), Entity.EntityType.ROLE));
    assertEquals(0, RoleMetaService.getInstance().listRolesByUserId(user.id()).size());
    assertEquals(0, RoleMetaService.getInstance().listRolesByGroupId(group.id()).size());
    assertTrue(backend.exists(anotherRole.nameIdentifier(), Entity.EntityType.ROLE));

    assertFalse(backend.exists(user.nameIdentifier(), Entity.EntityType.USER));
    assertEquals(
        0,
        SessionUtils.doWithCommitAndFetchResult(
                UserMetaMapper.class, mapper -> mapper.listUsersByRoleId(role.id()))
            .size());
    assertTrue(backend.exists(anotherUser.nameIdentifier(), Entity.EntityType.USER));

    assertFalse(backend.exists(group.nameIdentifier(), Entity.EntityType.GROUP));
    assertEquals(
        0,
        SessionUtils.doWithCommitAndFetchResult(
                GroupMetaMapper.class, mapper -> mapper.listGroupsByRoleId(role.id()))
            .size());
    assertTrue(backend.exists(anotherGroup.nameIdentifier(), Entity.EntityType.GROUP));

    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(metalake.id(), Entity.EntityType.METALAKE));
    assertTrue(legacyRecordExistsInDB(catalog.id(), Entity.EntityType.CATALOG));
    assertTrue(legacyRecordExistsInDB(schema.id(), Entity.EntityType.SCHEMA));
    assertTrue(legacyRecordExistsInDB(table.id(), Entity.EntityType.TABLE));
    assertTrue(legacyRecordExistsInDB(topic.id(), Entity.EntityType.TOPIC));
    assertTrue(legacyRecordExistsInDB(fileset.id(), Entity.EntityType.FILESET));
    assertTrue(legacyRecordExistsInDB(role.id(), Entity.EntityType.ROLE));
    assertTrue(legacyRecordExistsInDB(user.id(), Entity.EntityType.USER));
    assertTrue(legacyRecordExistsInDB(group.id(), Entity.EntityType.GROUP));
    assertEquals(2, countRoleRels(role.id()));
    assertEquals(2, countRoleRels(anotherRole.id()));
    assertEquals(2, listFilesetVersions(fileset.id()).size());
    assertEquals(3, listFilesetVersions(anotherFileset.id()).size());

    // meta data hard delete
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.hardDeleteLegacyData(entityType, Instant.now().toEpochMilli() + 1000);
    }
    assertFalse(legacyRecordExistsInDB(metalake.id(), Entity.EntityType.METALAKE));
    assertFalse(legacyRecordExistsInDB(catalog.id(), Entity.EntityType.CATALOG));
    assertFalse(legacyRecordExistsInDB(schema.id(), Entity.EntityType.SCHEMA));
    assertFalse(legacyRecordExistsInDB(table.id(), Entity.EntityType.TABLE));
    assertFalse(legacyRecordExistsInDB(fileset.id(), Entity.EntityType.FILESET));
    assertFalse(legacyRecordExistsInDB(topic.id(), Entity.EntityType.TOPIC));
    assertFalse(legacyRecordExistsInDB(role.id(), Entity.EntityType.ROLE));
    assertFalse(legacyRecordExistsInDB(user.id(), Entity.EntityType.USER));
    assertFalse(legacyRecordExistsInDB(group.id(), Entity.EntityType.GROUP));
    assertEquals(0, countRoleRels(role.id()));
    assertEquals(2, countRoleRels(anotherRole.id()));
    assertEquals(0, listFilesetVersions(fileset.id()).size());

    // soft delete for old version fileset
    assertEquals(3, listFilesetVersions(anotherFileset.id()).size());
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.deleteOldVersionData(entityType, 1);
    }
    Map<Integer, Long> versionDeletedMap = listFilesetVersions(anotherFileset.id());
    assertEquals(3, versionDeletedMap.size());
    assertEquals(1, versionDeletedMap.values().stream().filter(value -> value == 0L).count());
    assertEquals(2, versionDeletedMap.values().stream().filter(value -> value != 0L).count());

    // hard delete for old version fileset
    backend.hardDeleteLegacyData(Entity.EntityType.FILESET, Instant.now().toEpochMilli() + 1000);
    assertEquals(1, listFilesetVersions(anotherFileset.id()).size());
  }

  private boolean legacyRecordExistsInDB(Long id, Entity.EntityType entityType) {
    String tableName;
    String idColumnName;

    switch (entityType) {
      case METALAKE:
        tableName = "metalake_meta";
        idColumnName = "metalake_id";
        break;
      case CATALOG:
        tableName = "catalog_meta";
        idColumnName = "catalog_id";
        break;
      case SCHEMA:
        tableName = "schema_meta";
        idColumnName = "schema_id";
        break;
      case TABLE:
        tableName = "table_meta";
        idColumnName = "table_id";
        break;
      case FILESET:
        tableName = "fileset_meta";
        idColumnName = "fileset_id";
        break;
      case TOPIC:
        tableName = "topic_meta";
        idColumnName = "topic_id";
        break;
      case ROLE:
        tableName = "role_meta";
        idColumnName = "role_id";
        break;
      case USER:
        tableName = "user_meta";
        idColumnName = "user_id";
        break;
      case GROUP:
        tableName = "group_meta";
        idColumnName = "group_id";
        break;
      default:
        throw new IllegalArgumentException("Unsupported entity type: " + entityType);
    }

    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT * FROM %s WHERE %s = %d AND deleted_at != 0",
                    tableName, idColumnName, id))) {
      return rs.next();
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  private Map<Integer, Long> listFilesetVersions(Long filesetId) {
    Map<Integer, Long> versionDeletedTime = new HashMap<>();
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT version, deleted_at FROM fileset_version_info WHERE fileset_id = %d",
                    filesetId))) {
      while (rs.next()) {
        versionDeletedTime.put(rs.getInt("version"), rs.getLong("deleted_at"));
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return versionDeletedTime;
  }

  private Integer countRoleRels(Long roleId) {
    int count = 0;
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format("SELECT count(*) FROM user_role_rel WHERE role_id = %d", roleId));
        Statement statement2 = connection.createStatement();
        ResultSet rs2 =
            statement2.executeQuery(
                String.format("SELECT count(*) FROM group_role_rel WHERE role_id = %d", roleId))) {
      while (rs1.next()) {
        count += rs1.getInt(1);
      }
      while (rs2.next()) {
        count += rs2.getInt(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return count;
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
        .withProperties(new HashMap<>())
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

  public static UserEntity createUserEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return UserEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withRoleNames(null)
        .withRoleIds(null)
        .withAuditInfo(auditInfo)
        .build();
  }

  public static UserEntity createUserEntity(
      Long id,
      Namespace namespace,
      String name,
      AuditInfo auditInfo,
      List<String> roleNames,
      List<Long> roleIds) {
    return UserEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withRoleNames(roleNames)
        .withRoleIds(roleIds)
        .withAuditInfo(auditInfo)
        .build();
  }

  public static RoleEntity createRoleEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo, String catalogName) {
    SecurableObject securableObject =
        SecurableObjects.ofCatalog(catalogName, Lists.newArrayList(Privileges.UseCatalog.allow()));

    return RoleEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withProperties(null)
        .withAuditInfo(auditInfo)
        .withSecurableObjects(Lists.newArrayList(securableObject))
        .build();
  }

  public static GroupEntity createGroupEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return GroupEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withRoleNames(null)
        .withRoleIds(null)
        .withAuditInfo(auditInfo)
        .build();
  }

  public static GroupEntity createGroupEntity(
      Long id,
      Namespace namespace,
      String name,
      AuditInfo auditInfo,
      List<String> roleNames,
      List<Long> roleIds) {
    return GroupEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withRoleNames(roleNames)
        .withRoleIds(roleIds)
        .withAuditInfo(auditInfo)
        .build();
  }

  public static RoleEntity createRoleEntity(
      Long id,
      Namespace namespace,
      String name,
      AuditInfo auditInfo,
      SecurableObject securableObject,
      Map<String, String> properties) {

    return createRoleEntity(
        id, namespace, name, auditInfo, Lists.newArrayList(securableObject), properties);
  }

  public static RoleEntity createRoleEntity(
      Long id,
      Namespace namespace,
      String name,
      AuditInfo auditInfo,
      List<SecurableObject> securableObjects,
      Map<String, String> properties) {
    return RoleEntity.builder()
        .withId(id)
        .withName(name)
        .withProperties(properties)
        .withNamespace(namespace)
        .withAuditInfo(auditInfo)
        .withSecurableObjects(securableObjects)
        .build();
  }
}
