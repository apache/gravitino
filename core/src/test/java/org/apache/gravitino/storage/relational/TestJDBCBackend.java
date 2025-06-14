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

import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.SupportsRelationOperations.Type.OWNER_REL;
import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.GroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.service.CatalogMetaService;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.RoleMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestJDBCBackend {
  private final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_entityStore_" + UUID.randomUUID().toString().replace("-", "");
  private final String DB_DIR = JDBC_STORE_PATH + "/testdb";
  private final String H2_FILE = DB_DIR + ".mv.db";
  private final Config config = Mockito.mock(Config.class);
  public final ImmutableMap<String, String> RELATIONAL_BACKENDS =
      ImmutableMap.of(
          Configs.DEFAULT_ENTITY_RELATIONAL_STORE, JDBCBackend.class.getCanonicalName());
  protected RelationalBackend backend;

  @BeforeAll
  public void setup() {
    File dir = new File(DB_DIR);
    dir.deleteOnExit();
    if (dir.exists() || !dir.isDirectory()) {
      dir.delete();
    }
    dir.mkdirs();
    Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", DB_DIR));
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("123456");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);

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
  }

  @AfterAll
  public void tearDown() throws IOException {
    dropAllTables();
    backend.close();
    File dir = new File(DB_DIR);
    if (dir.exists()) {
      Files.delete(Paths.get(DB_DIR));
    }

    Files.delete(Paths.get(H2_FILE));
    Files.delete(Paths.get(JDBC_STORE_PATH));
  }

  @BeforeEach
  public void init() {
    truncateAllTables();
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
  public void testInsertAlreadyExistsException() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
    BaseMetalake metalakeCopy =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
    backend.insert(metalake, false);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(metalakeCopy, false));

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
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(catalogCopy, false));

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
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(schemaCopy, false));

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
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(tableCopy, false));

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
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(filesetCopy, false));

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
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(topicCopy, false));

    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofModel("metalake", "catalog", "schema"),
            "model",
            "model comment",
            1,
            ImmutableMap.of("key", "value"),
            auditInfo);
    ModelEntity modelCopy =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofModel("metalake", "catalog", "schema"),
            "model",
            "model comment",
            1,
            ImmutableMap.of("key", "value"),
            auditInfo);

    assertDoesNotThrow(() -> backend.insert(model, false));
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(modelCopy, false));
    assertDoesNotThrow(() -> backend.insert(modelCopy, true));
  }

  @Test
  public void testUpdateAlreadyExistsException() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
    BaseMetalake metalakeCopy =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake1", auditInfo);
    backend.insert(metalake, false);
    backend.insert(metalakeCopy, false);
    assertThrows(
        EntityAlreadyExistsException.class,
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
        EntityAlreadyExistsException.class,
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
        EntityAlreadyExistsException.class,
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
        EntityAlreadyExistsException.class,
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
        EntityAlreadyExistsException.class,
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
        EntityAlreadyExistsException.class,
        () ->
            backend.update(
                topicCopy.nameIdentifier(),
                Entity.EntityType.TOPIC,
                e -> createTopicEntity(topicCopy.id(), topicCopy.namespace(), "topic", auditInfo)));
  }

  @Test
  void testUpdateMetalakeWithNullableComment() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("metalake" + RandomIdGenerator.INSTANCE.nextId())
            .withAuditInfo(auditInfo)
            .withComment(null)
            .withProperties(null)
            .withVersion(SchemaVersion.V_0_1)
            .build();

    backend.insert(metalake, false);

    backend.update(
        metalake.nameIdentifier(),
        Entity.EntityType.METALAKE,
        e ->
            BaseMetalake.builder()
                .withId(metalake.id())
                .withName(metalake.name())
                .withAuditInfo(auditInfo)
                .withComment("comment")
                .withProperties(metalake.properties())
                .withVersion(metalake.getVersion())
                .build());

    BaseMetalake updatedMetalake =
        backend.get(metalake.nameIdentifier(), Entity.EntityType.METALAKE);
    Assertions.assertNotNull(updatedMetalake.comment());

    backend.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, false);
  }

  @Test
  void testUpdateCatalogWithNullableComment() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    String metalakeName = "metalake" + RandomIdGenerator.INSTANCE.nextId();
    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(metalakeName)
            .withAuditInfo(auditInfo)
            .withComment("")
            .withProperties(null)
            .withVersion(SchemaVersion.V_0_1)
            .build();
    backend.insert(metalake, false);

    CatalogEntity catalog =
        CatalogEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withNamespace(NamespaceUtil.ofCatalog(metalakeName))
            .withName("catalog")
            .withAuditInfo(auditInfo)
            .withComment(null)
            .withProperties(null)
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .build();

    backend.insert(catalog, false);

    backend.update(
        catalog.nameIdentifier(),
        Entity.EntityType.CATALOG,
        e ->
            CatalogEntity.builder()
                .withId(catalog.id())
                .withNamespace(catalog.namespace())
                .withName(catalog.name())
                .withAuditInfo(auditInfo)
                .withComment("comment")
                .withProperties(catalog.getProperties())
                .withType(Catalog.Type.RELATIONAL)
                .withProvider("test")
                .build());

    CatalogEntity updatedCatalog = backend.get(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    Assertions.assertNotNull(updatedCatalog.getComment());

    backend.delete(catalog.nameIdentifier(), Entity.EntityType.CATALOG, false);
    backend.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, false);
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

    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofModel("metalake", "catalog", "schema"),
            "model",
            "model comment",
            1,
            ImmutableMap.of("key", "value"),
            auditInfo);
    backend.insert(model, false);

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

    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag")
            .withNamespace(NamespaceUtil.ofTag("metalake"))
            .withComment("tag comment")
            .withAuditInfo(auditInfo)
            .build();
    backend.insert(tag, false);

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

    TagEntity anotherTagEntity =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("another-tag")
            .withNamespace(NamespaceUtil.ofTag("another-metalake"))
            .withComment("another-tag comment")
            .withAuditInfo(auditInfo)
            .build();
    backend.insert(anotherTagEntity, false);

    // meta data list
    List<BaseMetalake> metaLakes =
        backend.list(metalake.namespace(), Entity.EntityType.METALAKE, true);
    assertTrue(metaLakes.contains(metalake));

    List<CatalogEntity> catalogs =
        backend.list(catalog.namespace(), Entity.EntityType.CATALOG, true);
    assertTrue(catalogs.contains(catalog));

    assertEquals(
        1,
        SessionUtils.doWithCommitAndFetchResult(
                CatalogMetaMapper.class,
                mapper -> mapper.listCatalogPOsByMetalakeName(metalake.name()))
            .size());

    List<SchemaEntity> schemas = backend.list(schema.namespace(), Entity.EntityType.SCHEMA, true);
    assertTrue(schemas.contains(schema));

    List<TableEntity> tables = backend.list(table.namespace(), Entity.EntityType.TABLE, true);
    assertTrue(tables.contains(table));

    List<FilesetEntity> filesets =
        backend.list(fileset.namespace(), Entity.EntityType.FILESET, true);
    assertFalse(filesets.contains(fileset));
    assertTrue(filesets.contains(filesetV2));
    assertEquals("2", filesets.get(filesets.indexOf(filesetV2)).properties().get("version"));

    List<TopicEntity> topics = backend.list(topic.namespace(), Entity.EntityType.TOPIC, true);
    assertTrue(topics.contains(topic));

    List<ModelEntity> models = backend.list(model.namespace(), Entity.EntityType.MODEL, true);
    assertTrue(models.contains(model));

    RoleEntity roleEntity = backend.get(role.nameIdentifier(), Entity.EntityType.ROLE);
    assertEquals(role, roleEntity);
    assertEquals(1, RoleMetaService.getInstance().listRolesByUserId(user.id()).size());
    assertEquals(1, RoleMetaService.getInstance().listRolesByGroupId(group.id()).size());

    CatalogEntity catalogEntity = backend.get(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    assertEquals(catalog, catalogEntity);
    assertNotNull(
        CatalogMetaService.getInstance()
            .getCatalogPOByName(catalogEntity.namespace().level(0), catalog.name()));
    assertEquals(
        catalog.id(),
        CatalogMetaService.getInstance()
            .getCatalogIdByName(catalog.namespace().level(0), catalog.name()));

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

    TagEntity tagEntity = backend.get(tag.nameIdentifier(), Entity.EntityType.TAG);
    assertEquals(tag, tagEntity);
    List<TagEntity> tags = backend.list(tag.namespace(), Entity.EntityType.TAG, true);
    assertTrue(tags.contains(tag));
    assertEquals(1, tags.size());

    backend.insertRelation(
        OWNER_REL,
        metalake.nameIdentifier(),
        metalake.type(),
        user.nameIdentifier(),
        user.type(),
        true);

    backend.insertRelation(
        OWNER_REL,
        anotherMetaLake.nameIdentifier(),
        anotherMetaLake.type(),
        anotherUser.nameIdentifier(),
        anotherUser.type(),
        true);

    backend.insertRelation(
        OWNER_REL,
        catalog.nameIdentifier(),
        catalog.type(),
        user.nameIdentifier(),
        user.type(),
        true);

    backend.insertRelation(
        OWNER_REL,
        schema.nameIdentifier(),
        schema.type(),
        user.nameIdentifier(),
        user.type(),
        true);

    backend.insertRelation(
        OWNER_REL, table.nameIdentifier(), table.type(), user.nameIdentifier(), user.type(), true);

    backend.insertRelation(
        OWNER_REL, topic.nameIdentifier(), topic.type(), user.nameIdentifier(), user.type(), true);

    backend.insertRelation(
        OWNER_REL,
        fileset.nameIdentifier(),
        fileset.type(),
        user.nameIdentifier(),
        user.type(),
        true);

    backend.insertRelation(
        OWNER_REL, role.nameIdentifier(), role.type(), user.nameIdentifier(), user.type(), true);

    // meta data soft delete
    backend.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, true);

    assertEquals(
        0,
        SessionUtils.doWithCommitAndFetchResult(
                CatalogMetaMapper.class,
                mapper -> mapper.listCatalogPOsByMetalakeName(metalake.name()))
            .size());

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
    assertFalse(backend.exists(model.nameIdentifier(), Entity.EntityType.MODEL));

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

    assertFalse(backend.exists(tag.nameIdentifier(), Entity.EntityType.TAG));
    assertTrue(backend.exists(anotherTagEntity.nameIdentifier(), Entity.EntityType.TAG));

    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(metalake.id(), Entity.EntityType.METALAKE));
    assertTrue(legacyRecordExistsInDB(catalog.id(), Entity.EntityType.CATALOG));
    assertTrue(legacyRecordExistsInDB(schema.id(), Entity.EntityType.SCHEMA));
    assertTrue(legacyRecordExistsInDB(table.id(), Entity.EntityType.TABLE));
    assertTrue(legacyRecordExistsInDB(topic.id(), Entity.EntityType.TOPIC));
    assertTrue(legacyRecordExistsInDB(model.id(), Entity.EntityType.MODEL));
    assertTrue(legacyRecordExistsInDB(fileset.id(), Entity.EntityType.FILESET));
    assertTrue(legacyRecordExistsInDB(role.id(), Entity.EntityType.ROLE));
    assertTrue(legacyRecordExistsInDB(user.id(), Entity.EntityType.USER));
    assertTrue(legacyRecordExistsInDB(group.id(), Entity.EntityType.GROUP));
    assertEquals(2, countRoleRels(role.id()));
    assertEquals(7, countOwnerRel(metalake.id()));
    assertEquals(1, countOwnerRel(anotherMetaLake.id()));
    assertEquals(2, countRoleRels(anotherRole.id()));
    assertEquals(2, listFilesetVersions(fileset.id()).size());
    assertEquals(3, listFilesetVersions(anotherFileset.id()).size());
    assertTrue(legacyRecordExistsInDB(tag.id(), Entity.EntityType.TAG));

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
    assertFalse(legacyRecordExistsInDB(model.id(), Entity.EntityType.MODEL));
    assertFalse(legacyRecordExistsInDB(role.id(), Entity.EntityType.ROLE));
    assertFalse(legacyRecordExistsInDB(user.id(), Entity.EntityType.USER));
    assertFalse(legacyRecordExistsInDB(group.id(), Entity.EntityType.GROUP));
    assertEquals(0, countRoleRels(role.id()));
    assertEquals(2, countRoleRels(anotherRole.id()));
    assertEquals(0, listFilesetVersions(fileset.id()).size());
    assertFalse(legacyRecordExistsInDB(tag.id(), Entity.EntityType.TAG));
    assertEquals(0, countOwnerRel(metalake.id()));
    assertEquals(1, countOwnerRel(anotherMetaLake.id()));

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

  @Test
  public void testGetRoleIdByMetalakeIdAndName() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    String metalakeName = "testMetalake";
    String catalogName = "catalog";
    String roleNameWithDot = "role.with.dot";
    String roleNameWithoutDot = "roleWithoutDot";

    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            catalogName,
            auditInfo);
    backend.insert(catalog, false);

    RoleEntity roleWithDot =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            roleNameWithDot,
            auditInfo,
            catalogName);
    backend.insert(roleWithDot, false);

    RoleEntity roleWithoutDot =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            roleNameWithoutDot,
            auditInfo,
            catalogName);
    backend.insert(roleWithoutDot, false);

    Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);

    Long roleIdWithDot =
        RoleMetaService.getInstance().getRoleIdByMetalakeIdAndName(metalakeId, roleNameWithDot);
    assertEquals(roleWithDot.id(), roleIdWithDot);

    Long roleIdWithoutDot =
        RoleMetaService.getInstance().getRoleIdByMetalakeIdAndName(metalakeId, roleNameWithoutDot);
    assertEquals(roleWithoutDot.id(), roleIdWithoutDot);
  }

  @Test
  public void testInsertRelationWithDotInRoleName() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    String metalakeName = "testMetalake";
    String catalogName = "catalog";
    String roleNameWithDot = "role.with.dot";

    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            catalogName,
            auditInfo);
    backend.insert(catalog, false);

    RoleEntity role =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            roleNameWithDot,
            auditInfo,
            catalogName);
    backend.insert(role, false);

    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user",
            auditInfo);
    backend.insert(user, false);

    backend.insertRelation(
        OWNER_REL, role.nameIdentifier(), role.type(), user.nameIdentifier(), user.type(), true);
    assertEquals(1, countActiveOwnerRel(user.id()));
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
      case MODEL:
        tableName = "model_meta";
        idColumnName = "model_id";
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
      case TAG:
        tableName = "tag_meta";
        idColumnName = "tag_id";
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

  private Integer countOwnerRel(Long metalakeId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format(
                    "SELECT count(*) FROM owner_meta WHERE metalake_id = %d", metalakeId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }

  protected Integer countAllObjectRel(Long roleId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format(
                    "SELECT count(*) FROM role_meta_securable_object WHERE role_id = %d",
                    roleId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }

  protected Integer countActiveObjectRel(Long roleId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format(
                    "SELECT count(*) FROM role_meta_securable_object WHERE role_id = %d AND deleted_at = 0",
                    roleId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }

  protected Integer countAllTagRel(Long tagId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format("SELECT count(*) FROM tag_relation_meta WHERE tag_id = %d", tagId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }

  protected Integer countActiveTagRel(Long tagId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format(
                    "SELECT count(*) FROM tag_relation_meta WHERE tag_id = %d AND deleted_at = 0",
                    tagId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }

  protected Integer countAllOwnerRel(Long ownerId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format("SELECT count(*) FROM owner_meta WHERE owner_id = %d", ownerId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }

  protected Integer countActiveOwnerRel(long ownerId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format(
                    "SELECT count(*) FROM owner_meta WHERE owner_id = %d AND deleted_at = 0",
                    ownerId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
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
        .withStorageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "/tmp"))
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

  public static ModelEntity createModelEntity(
      Long id,
      Namespace namespace,
      String name,
      String comment,
      Integer latestVersion,
      Map<String, String> properties,
      AuditInfo auditInfo) {
    return ModelEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment(comment)
        .withLatestVersion(latestVersion)
        .withProperties(properties)
        .withAuditInfo(auditInfo)
        .build();
  }

  public static ModelVersionEntity createModelVersionEntity(
      NameIdentifier modelId,
      Integer version,
      String modelUri,
      List<String> aliases,
      String comment,
      Map<String, String> properties,
      AuditInfo auditInfo) {
    return ModelVersionEntity.builder()
        .withModelIdentifier(modelId)
        .withVersion(version)
        .withUri(modelUri)
        .withAliases(aliases)
        .withComment(comment)
        .withProperties(properties)
        .withAuditInfo(auditInfo)
        .build();
  }

  protected void createParentEntities(
      String metalakeName, String catalogName, String schemaName, AuditInfo auditInfo)
      throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName),
            catalogName,
            auditInfo);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalog.name()),
            schemaName,
            auditInfo);
    backend.insert(schema, false);
  }
}
