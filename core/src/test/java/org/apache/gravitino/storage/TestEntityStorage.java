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

package org.apache.gravitino.storage;

import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.Configs.VERSION_RETENTION_COUNT;
import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.relational.RelationalBackend;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.RelationalGarbageCollector;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.converters.H2ExceptionConverter;
import org.apache.gravitino.storage.relational.converters.MySQLExceptionConverter;
import org.apache.gravitino.storage.relational.converters.PostgreSQLExceptionConverter;
import org.apache.gravitino.storage.relational.converters.SQLExceptionConverterFactory;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Pair;

@Tag("gravitino-docker-test")
public class TestEntityStorage {
  private static final Logger LOG = LoggerFactory.getLogger(TestEntityStorage.class);

  private static final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_entityStore_" + UUID.randomUUID().toString().replace("-", "");
  private static final String DB_DIR = JDBC_STORE_PATH + "/testdb";
  private static final String H2_FILE = DB_DIR + ".mv.db";

  static Object[] storageProvider() {
    return new Object[] {"h2", "mysql", "postgresql"};
  }

  @AfterEach
  void closeSuit() throws IOException {
    ContainerSuite.getInstance().close();
  }

  private void init(String type, Config config) {
    Preconditions.checkArgument(StringUtils.isNotBlank(type));
    File dir = new File(DB_DIR);
    if (dir.exists() || !dir.isDirectory()) {
      dir.delete();
    }
    dir.mkdirs();
    Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PATH)).thenReturn(DB_DIR);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);
    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    Mockito.when(config.get(VERSION_RETENTION_COUNT)).thenReturn(1L);
    // Fix cache config for test
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_MAX_ENTRIES)).thenReturn(10_000);
    Mockito.when(config.get(Configs.CACHE_EXPIRATION_TIME)).thenReturn(3_600_000L);
    Mockito.when(config.get(Configs.CACHE_WEIGHER_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_STATS_ENABLED)).thenReturn(false);
    Mockito.when(config.get(Configs.CACHE_IMPLEMENTATION)).thenReturn("caffeine");

    BaseIT baseIT = new BaseIT();

    try {
      if (type.equalsIgnoreCase("h2")) {
        // The following properties are used to create the JDBC connection; they are just for test,
        // in the real world, they will be set automatically by the configuration file if you set
        // ENTITY_RELATIONAL_STORE as EMBEDDED_ENTITY_RELATIONAL_STORE.
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
            .thenReturn(String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", DB_DIR));
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("gravitino");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("gravitino");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS))
            .thenReturn(1000L);

        FieldUtils.writeStaticField(
            SQLExceptionConverterFactory.class, "converter", new H2ExceptionConverter(), true);

      } else if (type.equalsIgnoreCase("mysql")) {
        String mysqlJdbcUrl = baseIT.startAndInitMySQLBackend();
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL)).thenReturn(mysqlJdbcUrl);
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("root");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER))
            .thenReturn("com.mysql.cj.jdbc.Driver");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS))
            .thenReturn(1000L);

        FieldUtils.writeStaticField(
            SQLExceptionConverterFactory.class, "converter", new MySQLExceptionConverter(), true);

      } else if (type.equalsIgnoreCase("postgresql")) {
        String postgreSQLJdbcUrl = baseIT.startAndInitPGBackend();
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL)).thenReturn(postgreSQLJdbcUrl);
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("root");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER))
            .thenReturn("org.postgresql.Driver");

        FieldUtils.writeStaticField(
            SQLExceptionConverterFactory.class,
            "converter",
            new PostgreSQLExceptionConverter(),
            true);

        RelationalEntityStore store =
            (RelationalEntityStore) EntityStoreFactory.createEntityStore(config);
        store.initialize(config);
        Field f = FieldUtils.getField(RelationalEntityStore.class, "backend", true);
        RelationalBackend backend = (RelationalBackend) f.get(store);
        RelationalGarbageCollector garbageCollector =
            new RelationalGarbageCollector(backend, config);
        garbageCollector.collectAndClean();

      } else {
        throw new UnsupportedOperationException("Unsupported entity store type: " + type);
      }
    } catch (Exception e) {
      LOG.error("Failed to init entity store", e);
      throw new RuntimeException(e);
    }
  }

  private void destroy(String type) {
    Preconditions.checkArgument(StringUtils.isNotBlank(type));
    if (type.equalsIgnoreCase("h2") || type.equalsIgnoreCase("mysql")) {
      dropAllTables();
      File dir = new File(DB_DIR);
      if (dir.exists()) {
        dir.delete();
      }

      FileUtils.deleteQuietly(new File(H2_FILE));
    } else if (type.equalsIgnoreCase("postgresql")) {
      // Do nothing
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
      TopicEntity topic1 =
          createTopicEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema1"),
              "topic1",
              auditInfo);
      ModelEntity model1 =
          TestJDBCBackend.createModelEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema1"),
              "model1",
              "model1",
              0,
              null,
              auditInfo);
      ModelVersionEntity modelVersion1 =
          TestJDBCBackend.createModelVersionEntity(
              model1.nameIdentifier(),
              0,
              "model_path",
              ImmutableList.of("alias1", "alias2"),
              null,
              null,
              auditInfo);

      UserEntity user1 =
          createUser(RandomIdGenerator.INSTANCE.nextId(), "metalake", "user1", auditInfo);
      GroupEntity group1 =
          createGroup(RandomIdGenerator.INSTANCE.nextId(), "metalake", "group1", auditInfo);
      RoleEntity role1 =
          createRole(RandomIdGenerator.INSTANCE.nextId(), "metalake", "role1", auditInfo);

      // Store all entities
      store.put(metalake);
      store.put(catalog);
      store.put(catalogCopy);
      store.put(schema1);
      store.put(table1);
      store.put(fileset1);
      store.put(topic1);
      store.put(model1);
      store.put(modelVersion1);
      store.put(user1);
      store.put(group1);
      store.put(role1);

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
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "topic1"),
                  Entity.EntityType.TOPIC,
                  TopicEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "model1"),
                  Entity.EntityType.MODEL,
                  ModelEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "model1", "0"),
                  Entity.EntityType.MODEL_VERSION,
                  ModelVersionEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "model1", "alias1"),
                  EntityType.MODEL_VERSION,
                  ModelVersionEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "model1", "alias2"),
                  EntityType.MODEL_VERSION,
                  ModelVersionEntity.class));

      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  AuthorizationUtils.ofUser("metalake", "user1"),
                  Entity.EntityType.USER,
                  UserEntity.class));

      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  AuthorizationUtils.ofGroup("metalake", "group1"),
                  EntityType.GROUP,
                  GroupEntity.class));

      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  AuthorizationUtils.ofRole("metalake", "role1"),
                  EntityType.ROLE,
                  RoleEntity.class));
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
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "topic1"),
                  Entity.EntityType.TOPIC,
                  TopicEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "model1"),
                  Entity.EntityType.MODEL,
                  ModelEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "model1", "0"),
                  Entity.EntityType.MODEL_VERSION,
                  ModelVersionEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "model1", "alias1"),
                  EntityType.MODEL_VERSION,
                  ModelVersionEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "model1", "alias2"),
                  EntityType.MODEL_VERSION,
                  ModelVersionEntity.class));

      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  AuthorizationUtils.ofUser("metalake", "user1"),
                  Entity.EntityType.USER,
                  UserEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  AuthorizationUtils.ofGroup("metalake", "group1"),
                  Entity.EntityType.GROUP,
                  GroupEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  AuthorizationUtils.ofRole("metalake", "role1"),
                  Entity.EntityType.ROLE,
                  RoleEntity.class));
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
      TopicEntity topic1 =
          createTopicEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema1"),
              "topic1",
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
      TopicEntity topic1InSchema2 =
          createTopicEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema2"),
              "topic1",
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
      store.put(topic1);
      store.put(topic1InSchema2);

      validateMetalakeChanged(store, metalake);
      validateCatalogChanged(store, catalog);
      validateSchemaChanged(store, schema1);
      validateTableChanged(store, table1);
      validateFilesetChanged(store, fileset1);
      validateTopicChanged(store, topic1);
      validateDeletedTable(store);
      validateDeletedFileset(store);
      validateDeletedTopic(store);
      validateAlreadyExistEntity(store, schema2);
      validateNotChangedEntity(store, schema2);

      destroy(type);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  public void testAuthorizationEntityDelete(String type) throws IOException {
    Config config = Mockito.mock(Config.class);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);

      BaseMetalake metalake = createBaseMakeLake(1L, "metalake", auditInfo);
      store.put(metalake);
      CatalogEntity catalog = createCatalog(1L, Namespace.of("metalake"), "catalog", auditInfo);
      store.put(catalog);
      UserEntity oneUser = createUser(1L, "metalake", "oneUser", auditInfo);
      store.put(oneUser);
      UserEntity anotherUser = createUser(2L, "metalake", "anotherUser", auditInfo);
      store.put(anotherUser);
      GroupEntity oneGroup = createGroup(1L, "metalake", "oneGroup", auditInfo);
      store.put(oneGroup);
      GroupEntity anotherGroup = createGroup(2L, "metalake", "anotherGroup", auditInfo);
      store.put(anotherGroup);
      RoleEntity oneRole = createRole(1L, "metalake", "oneRole", auditInfo);
      store.put(oneRole);
      RoleEntity anotherRole = createRole(2L, "metalake", "anotherRole", auditInfo);
      store.put(anotherRole);
      Assertions.assertTrue(store.exists(oneUser.nameIdentifier(), Entity.EntityType.USER));
      Assertions.assertTrue(store.exists(anotherUser.nameIdentifier(), Entity.EntityType.USER));
      Assertions.assertTrue(store.exists(oneGroup.nameIdentifier(), Entity.EntityType.GROUP));
      Assertions.assertTrue(store.exists(anotherGroup.nameIdentifier(), Entity.EntityType.GROUP));
      Assertions.assertTrue(store.exists(oneRole.nameIdentifier(), Entity.EntityType.ROLE));
      Assertions.assertTrue(store.exists(anotherRole.nameIdentifier(), Entity.EntityType.ROLE));

      store.delete(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
      store.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE);
      Assertions.assertFalse(store.exists(oneUser.nameIdentifier(), Entity.EntityType.USER));
      Assertions.assertFalse(store.exists(anotherUser.nameIdentifier(), Entity.EntityType.USER));
      Assertions.assertFalse(store.exists(oneGroup.nameIdentifier(), Entity.EntityType.GROUP));
      Assertions.assertFalse(store.exists(anotherGroup.nameIdentifier(), Entity.EntityType.GROUP));
      Assertions.assertFalse(store.exists(oneRole.nameIdentifier(), Entity.EntityType.ROLE));
      Assertions.assertFalse(store.exists(anotherRole.nameIdentifier(), Entity.EntityType.ROLE));
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testEntityDelete(String type) throws IOException {
    Config config = Mockito.mock(Config.class);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);

      BaseMetalake metalake = createBaseMakeLake(1L, "metalake", auditInfo);
      CatalogEntity catalog = createCatalog(1L, Namespace.of("metalake"), "catalog", auditInfo);
      CatalogEntity catalogCopy =
          createCatalog(2L, Namespace.of("metalake"), "catalogCopy", auditInfo);

      SchemaEntity schema1 =
          createSchemaEntity(1L, Namespace.of("metalake", "catalog"), "schema1", auditInfo);
      ColumnEntity column1 =
          createColumnEntity(
              RandomIdGenerator.INSTANCE.nextId(), "column1", 0, Types.StringType.get(), auditInfo);
      ColumnEntity column2 =
          createColumnEntity(
              RandomIdGenerator.INSTANCE.nextId(), "column2", 1, Types.StringType.get(), auditInfo);
      TableEntity table1 =
          createTableEntityWithColumns(
              1L,
              Namespace.of("metalake", "catalog", "schema1"),
              "table1",
              auditInfo,
              Lists.newArrayList(column1, column2));
      FilesetEntity fileset1 =
          createFilesetEntity(
              1L, Namespace.of("metalake", "catalog", "schema1"), "fileset1", auditInfo);
      TopicEntity topic1 =
          createTopicEntity(
              1L, Namespace.of("metalake", "catalog", "schema1"), "topic1", auditInfo);
      ModelEntity model1 =
          TestJDBCBackend.createModelEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema1"),
              "model1",
              "model1",
              0,
              null,
              auditInfo);
      ModelVersionEntity modelVersion1 =
          TestJDBCBackend.createModelVersionEntity(
              model1.nameIdentifier(),
              0,
              "model_path",
              ImmutableList.of("alias1", "alias2"),
              null,
              null,
              auditInfo);

      SchemaEntity schema2 =
          createSchemaEntity(2L, Namespace.of("metalake", "catalog"), "schema2", auditInfo);
      ColumnEntity column3 =
          createColumnEntity(
              RandomIdGenerator.INSTANCE.nextId(), "column3", 2, Types.StringType.get(), auditInfo);
      ColumnEntity column4 =
          createColumnEntity(
              RandomIdGenerator.INSTANCE.nextId(), "column4", 3, Types.StringType.get(), auditInfo);
      TableEntity table1InSchema2 =
          createTableEntityWithColumns(
              2L,
              Namespace.of("metalake", "catalog", "schema2"),
              "table1",
              auditInfo,
              Lists.newArrayList(column3, column4));
      FilesetEntity fileset1InSchema2 =
          createFilesetEntity(
              2L, Namespace.of("metalake", "catalog", "schema2"), "fileset1", auditInfo);
      TopicEntity topic1InSchema2 =
          createTopicEntity(
              2L, Namespace.of("metalake", "catalog", "schema2"), "topic1", auditInfo);
      ModelEntity model1InSchema2 =
          TestJDBCBackend.createModelEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema2"),
              "model1",
              "model1",
              0,
              null,
              auditInfo);
      ModelVersionEntity modelVersion1InSchema2 =
          TestJDBCBackend.createModelVersionEntity(
              model1InSchema2.nameIdentifier(),
              0,
              "model_path",
              ImmutableList.of("alias1", "alias2"),
              null,
              null,
              auditInfo);

      UserEntity user1 = createUser(1L, "metalake", "user1", auditInfo);
      UserEntity user2 = createUser(2L, "metalake", "user2", auditInfo);
      GroupEntity group1 = createGroup(1L, "metalake", "group1", auditInfo);
      GroupEntity group2 = createGroup(2L, "metalake", "group2", auditInfo);
      RoleEntity role1 = createRole(1L, "metalake", "role1", auditInfo);
      RoleEntity role2 = createRole(2L, "metalake", "role2", auditInfo);

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
      store.put(topic1);
      store.put(topic1InSchema2);
      store.put(model1);
      store.put(modelVersion1);
      store.put(model1InSchema2);
      store.put(modelVersion1InSchema2);
      store.put(user1);
      store.put(user2);
      store.put(group1);
      store.put(group2);
      store.put(role1);
      store.put(role2);

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
          fileset1InSchema2,
          topic1,
          topic1InSchema2,
          model1,
          modelVersion1,
          model1InSchema2,
          modelVersion1InSchema2,
          user1,
          user2,
          group1,
          group2,
          role1,
          role2);

      validateDeleteUser(store, user1);

      validateDeleteGroup(store, group1);

      validateDeleteRole(store, role1);

      validateDeleteTable(store, schema2, table1, table1InSchema2);

      validateDeleteFileset(store, schema2, fileset1, fileset1InSchema2);

      validateDeleteTopic(store, schema2, topic1, topic1InSchema2);

      validateDeleteModel(
          store, schema2, model1, modelVersion1, model1InSchema2, modelVersion1InSchema2);

      validateDeleteSchema(store, schema1, table1, fileset1, topic1, model1, modelVersion1);

      validateDeleteCatalog(
          store,
          catalog,
          table1,
          schema1,
          table1InSchema2,
          schema2,
          fileset1,
          fileset1InSchema2,
          topic1,
          topic1InSchema2,
          model1,
          model1InSchema2);

      validateDeleteMetalake(store, metalake, catalogCopy, user2, group2, role2);

      // Store all entities again
      // metalake
      BaseMetalake metalakeNew =
          createBaseMakeLake(
              RandomIdGenerator.INSTANCE.nextId(), metalake.name(), metalake.auditInfo());
      store.put(metalakeNew);
      // catalog
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
      // schema
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
      // table
      TableEntity table1New =
          createTableEntityWithColumns(
              RandomIdGenerator.INSTANCE.nextId(),
              table1.namespace(),
              table1.name(),
              table1.auditInfo(),
              table1.columns());
      store.put(table1New);
      TableEntity table1InSchema2New =
          createTableEntityWithColumns(
              RandomIdGenerator.INSTANCE.nextId(),
              table1InSchema2.namespace(),
              table1InSchema2.name(),
              table1InSchema2.auditInfo(),
              table1InSchema2.columns());
      store.put(table1InSchema2New);
      // fileset
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
      // topic
      TopicEntity topic1New =
          createTopicEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              topic1.namespace(),
              topic1.name(),
              topic1.auditInfo());
      store.put(topic1New);
      TopicEntity topic1InSchema2New =
          createTopicEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              topic1InSchema2.namespace(),
              topic1InSchema2.name(),
              topic1InSchema2.auditInfo());
      store.put(topic1InSchema2New);

      // model
      ModelEntity model1New =
          TestJDBCBackend.createModelEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              model1.namespace(),
              model1.name(),
              model1.comment(),
              model1.latestVersion(),
              model1.properties(),
              model1.auditInfo());
      store.put(model1New);

      UserEntity userNew =
          createUser(RandomIdGenerator.INSTANCE.nextId(), "metalake", "userNew", auditInfo);
      store.put(userNew);
      GroupEntity groupNew =
          createGroup(RandomIdGenerator.INSTANCE.nextId(), "metalake", "groupNew", auditInfo);
      store.put(groupNew);
      RoleEntity roleNew =
          createRole(RandomIdGenerator.INSTANCE.nextId(), "metalake", "roleNew", auditInfo);
      store.put(roleNew);

      validateDeleteTableCascade(store, table1New);

      validateDeleteFilesetCascade(store, fileset1New);

      validateDeleteTopicCascade(store, topic1New);

      validateDeleteModelCascade(store, model1New);

      validateDeleteSchemaCascade(store, schema1New, table1New, fileset1New, topic1New, model1New);

      validateDeleteCatalogCascade(store, catalogNew, schema2New);

      validateDeleteMetalakeCascade(
          store, metalakeNew, catalogNew, schema2New, userNew, groupNew, roleNew);

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

      TopicEntity topicEntity1 =
          createTopicEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, "sameName", auditInfo);

      ModelEntity model1 =
          TestJDBCBackend.createModelEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              namespace,
              "sameName",
              "model1",
              1,
              null,
              auditInfo);

      store.put(metalake1);
      store.put(catalog1);
      store.put(schema1);
      store.put(table1);
      store.put(filesetEntity1);
      store.put(topicEntity1);
      store.put(model1);

      NameIdentifier identifier = NameIdentifier.of("metalake1", "catalog1", "schema1", "sameName");

      TableEntity loadedTableEntity =
          store.get(identifier, Entity.EntityType.TABLE, TableEntity.class);
      Assertions.assertEquals(table1.id(), loadedTableEntity.id());
      FilesetEntity loadedFilesetEntity =
          store.get(identifier, Entity.EntityType.FILESET, FilesetEntity.class);
      Assertions.assertEquals(filesetEntity1.id(), loadedFilesetEntity.id());
      TopicEntity loadedTopicEntity =
          store.get(identifier, Entity.EntityType.TOPIC, TopicEntity.class);
      Assertions.assertEquals(topicEntity1.id(), loadedTopicEntity.id());
      ModelEntity loadedModelEntity =
          store.get(identifier, Entity.EntityType.MODEL, ModelEntity.class);
      Assertions.assertEquals(model1.id(), loadedModelEntity.id());

      // Remove table will not affect another
      Assertions.assertTrue(store.delete(identifier, Entity.EntityType.TABLE));
      Assertions.assertNotNull(
          store.get(identifier, Entity.EntityType.FILESET, FilesetEntity.class));
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.TOPIC, TopicEntity.class));
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.MODEL, ModelEntity.class));

      // JDBC use id as the primary key, so we need to change the id of table1 if we want to store
      // it again
      table1 =
          createTableEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, "sameName", auditInfo);
      store.put(table1);

      // Remove fileset will not affect another
      store.delete(identifier, Entity.EntityType.FILESET);
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.TABLE, TableEntity.class));
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.TOPIC, TopicEntity.class));
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.MODEL, ModelEntity.class));

      filesetEntity1 =
          createFilesetEntity(
              RandomIdGenerator.INSTANCE.nextId(), namespace, "sameName", auditInfo);
      store.put(filesetEntity1);

      // Remove topic will not affect another
      store.delete(identifier, Entity.EntityType.TOPIC);
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.TABLE, TableEntity.class));
      Assertions.assertNotNull(
          store.get(identifier, Entity.EntityType.FILESET, FilesetEntity.class));
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.MODEL, ModelEntity.class));

      topicEntity1 =
          createTopicEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, "sameName", auditInfo);
      store.put(topicEntity1);

      // Rename table will not affect another
      long table1Id = table1.id();
      store.update(
          identifier,
          TableEntity.class,
          Entity.EntityType.TABLE,
          e -> createTableEntity(table1Id, namespace, "sameNameChanged", e.auditInfo()));

      NameIdentifier changedNameIdentifier =
          NameIdentifier.of("metalake1", "catalog1", "schema1", "sameNameChanged");
      Assertions.assertNotNull(
          store.get(changedNameIdentifier, Entity.EntityType.TABLE, TableEntity.class));
      Assertions.assertNotNull(
          store.get(identifier, Entity.EntityType.FILESET, FilesetEntity.class));
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.TOPIC, TopicEntity.class));
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.MODEL, ModelEntity.class));

      table1 =
          createTableEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, "sameName", auditInfo);
      store.put(table1);

      // Rename fileset will not affect another
      long filesetId = filesetEntity1.id();
      store.update(
          identifier,
          FilesetEntity.class,
          Entity.EntityType.FILESET,
          e -> createFilesetEntity(filesetId, namespace, "sameNameChanged", e.auditInfo()));

      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.TABLE, TableEntity.class));
      Assertions.assertNotNull(
          store.get(changedNameIdentifier, Entity.EntityType.FILESET, FilesetEntity.class));
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.TOPIC, TopicEntity.class));
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.MODEL, ModelEntity.class));

      filesetEntity1 =
          createFilesetEntity(
              RandomIdGenerator.INSTANCE.nextId(), namespace, "sameName", auditInfo);
      store.put(filesetEntity1);

      // Rename topic will not affect another
      long topicId = topicEntity1.id();
      store.update(
          identifier,
          TopicEntity.class,
          Entity.EntityType.TOPIC,
          e -> createTopicEntity(topicId, namespace, "sameNameChanged", e.auditInfo()));

      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.TABLE, TableEntity.class));
      Assertions.assertNotNull(
          store.get(identifier, Entity.EntityType.FILESET, FilesetEntity.class));
      Assertions.assertNotNull(
          store.get(changedNameIdentifier, Entity.EntityType.TOPIC, TopicEntity.class));
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.MODEL, ModelEntity.class));

      destroy(type);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testDeleteAndRename(String type) throws IOException {
    Config config = Mockito.mock(Config.class);
    init(type, config);
    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);

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
          e -> createBaseMakeLake(metalake1New.id(), "metalake2", e.auditInfo()));

      // Rename metalake3 --> metalake1
      BaseMetalake metalake3New1 =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake3", auditInfo);
      store.put(metalake3New1);
      store.update(
          NameIdentifier.of("metalake3"),
          BaseMetalake.class,
          Entity.EntityType.METALAKE,
          e -> createBaseMakeLake(metalake3New1.id(), "metalake1", e.auditInfo()));

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
          e -> createBaseMakeLake(metalake3New2.id(), "metalake2", e.auditInfo()));

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

      // Test topic
      TopicEntity topic1 =
          createTopicEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake1", "catalog2", "schema2"),
              "topic1",
              auditInfo);
      TopicEntity topic2 =
          createTopicEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake1", "catalog2", "schema2"),
              "topic2",
              auditInfo);

      store.put(topic1);
      store.put(topic2);

      store.delete(
          NameIdentifier.of("metalake1", "catalog2", "schema2", "topic1"), Entity.EntityType.TOPIC);
      store.delete(
          NameIdentifier.of("metalake1", "catalog2", "schema2", "topic2"), Entity.EntityType.TOPIC);

      TopicEntity topic1New =
          createTopicEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              topic1.namespace(),
              topic1.name(),
              topic1.auditInfo());
      store.put(topic1New);
      store.update(
          topic1New.nameIdentifier(),
          TopicEntity.class,
          Entity.EntityType.TOPIC,
          e ->
              createTopicEntity(
                  topic1New.id(),
                  Namespace.of("metalake1", "catalog2", "schema2"),
                  "topic2",
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

  public static ColumnEntity createColumnEntity(
      Long id, String name, int position, Type dataType, AuditInfo auditInfo) {
    return ColumnEntity.builder()
        .withId(id)
        .withName(name)
        .withPosition(position)
        .withComment("")
        .withDataType(dataType)
        .withNullable(true)
        .withAutoIncrement(false)
        .withAuditInfo(auditInfo)
        .build();
  }

  public static TableEntity createTableEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return createTableEntityWithColumns(id, namespace, name, auditInfo, Collections.emptyList());
  }

  public static TableEntity createTableEntityWithColumns(
      Long id, Namespace namespace, String name, AuditInfo auditInfo, List<ColumnEntity> columns) {
    return TableEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(auditInfo)
        .withColumns(columns)
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

  private static UserEntity createUser(Long id, String metalake, String name, AuditInfo auditInfo) {
    return UserEntity.builder()
        .withId(id)
        .withNamespace(AuthorizationUtils.ofUserNamespace(metalake))
        .withName(name)
        .withAuditInfo(auditInfo)
        .withRoleNames(null)
        .build();
  }

  private static GroupEntity createGroup(
      Long id, String metalake, String name, AuditInfo auditInfo) {
    return GroupEntity.builder()
        .withId(id)
        .withNamespace(AuthorizationUtils.ofGroupNamespace(metalake))
        .withName(name)
        .withAuditInfo(auditInfo)
        .withRoleNames(null)
        .build();
  }

  private static RoleEntity createRole(Long id, String metalake, String name, AuditInfo auditInfo) {
    SecurableObject securableObject =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));

    return RoleEntity.builder()
        .withId(id)
        .withNamespace(AuthorizationUtils.ofRoleNamespace(metalake))
        .withName(name)
        .withAuditInfo(auditInfo)
        .withSecurableObjects(Lists.newArrayList(securableObject))
        .withProperties(null)
        .build();
  }

  private void validateDeleteTopicCascade(EntityStore store, TopicEntity topic1)
      throws IOException {
    // Delete the topic 'metalake.catalog.schema1.topic1'
    Assertions.assertTrue(store.delete(topic1.nameIdentifier(), Entity.EntityType.TOPIC));
    Assertions.assertFalse(store.exists(topic1.nameIdentifier(), Entity.EntityType.TOPIC));
    // Delete again should return false
    Assertions.assertFalse(store.delete(topic1.nameIdentifier(), Entity.EntityType.TOPIC));
  }

  private void validateDeleteModelCascade(EntityStore store, ModelEntity model1)
      throws IOException {
    // Delete the topic 'metalake.catalog.schema1.topic1'
    Assertions.assertTrue(store.delete(model1.nameIdentifier(), EntityType.MODEL));
    Assertions.assertFalse(store.exists(model1.nameIdentifier(), EntityType.MODEL));
    // Delete again should return false
    Assertions.assertFalse(store.delete(model1.nameIdentifier(), EntityType.MODEL));
  }

  private void validateDeleteFilesetCascade(EntityStore store, FilesetEntity fileset1)
      throws IOException {
    // Delete the fileset 'metalake.catalog.schema1.fileset1'
    Assertions.assertTrue(store.delete(fileset1.nameIdentifier(), Entity.EntityType.FILESET, true));
    Assertions.assertFalse(store.exists(fileset1.nameIdentifier(), Entity.EntityType.FILESET));
    // Delete again should return false
    Assertions.assertFalse(
        store.delete(fileset1.nameIdentifier(), Entity.EntityType.FILESET, true));
  }

  private void validateDeleteTableCascade(EntityStore store, TableEntity table1)
      throws IOException {
    // Delete the table 'metalake.catalog.schema1.table1'
    Assertions.assertTrue(store.delete(table1.nameIdentifier(), Entity.EntityType.TABLE, true));
    Assertions.assertFalse(store.exists(table1.nameIdentifier(), Entity.EntityType.TABLE));
    // Delete again should return false
    Assertions.assertFalse(store.delete(table1.nameIdentifier(), Entity.EntityType.TABLE, true));
    validateDeletedColumns(table1.id(), table1.type());
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
    // Delete again should return false
    Assertions.assertFalse(
        store.delete(fileset1InSchema2.nameIdentifier(), Entity.EntityType.FILESET));

    // Make sure fileset 'metalake.catalog.schema1.fileset1' still exist;
    Assertions.assertEquals(
        fileset1,
        store.get(fileset1.nameIdentifier(), Entity.EntityType.FILESET, FilesetEntity.class));
    // Make sure schema 'metalake.catalog.schema2' still exist;
    Assertions.assertEquals(
        schema2, store.get(schema2.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class));
  }

  private void validateDeleteTopic(
      EntityStore store, SchemaEntity schema2, TopicEntity topic1, TopicEntity topic1InSchema2)
      throws IOException {
    // Delete the topic 'metalake.catalog.schema2.topic1'
    Assertions.assertTrue(store.delete(topic1InSchema2.nameIdentifier(), Entity.EntityType.TOPIC));
    Assertions.assertFalse(store.exists(topic1InSchema2.nameIdentifier(), Entity.EntityType.TOPIC));
    // Delete again should return false
    Assertions.assertFalse(store.delete(topic1InSchema2.nameIdentifier(), Entity.EntityType.TOPIC));

    // Make sure topic 'metalake.catalog.schema1.topic1' still exist;
    Assertions.assertEquals(
        topic1, store.get(topic1.nameIdentifier(), Entity.EntityType.TOPIC, TopicEntity.class));
    // Make sure schema 'metalake.catalog.schema2' still exist;
    Assertions.assertEquals(
        schema2, store.get(schema2.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class));
  }

  private void validateDeleteMetalakeCascade(
      EntityStore store,
      BaseMetalake metalake,
      CatalogEntity catalog,
      SchemaEntity schema2,
      UserEntity userNew,
      GroupEntity groupNew,
      RoleEntity roleNew)
      throws IOException {
    Assertions.assertTrue(store.exists(userNew.nameIdentifier(), Entity.EntityType.USER));
    Assertions.assertTrue(store.exists(groupNew.nameIdentifier(), Entity.EntityType.GROUP));
    Assertions.assertTrue(store.exists(roleNew.nameIdentifier(), Entity.EntityType.ROLE));

    Assertions.assertTrue(
        store.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, true));

    // catalog has already deleted, so we can't delete it again and should return false
    Assertions.assertFalse(store.exists(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertFalse(store.exists(schema2.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(store.exists(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
    Assertions.assertFalse(store.exists(userNew.nameIdentifier(), Entity.EntityType.USER));
    Assertions.assertFalse(store.exists(groupNew.nameIdentifier(), EntityType.GROUP));
    Assertions.assertFalse(store.exists(roleNew.nameIdentifier(), EntityType.ROLE));
    validateDeletedColumns(metalake.id(), metalake.type());

    // Delete again should return false
    Assertions.assertFalse(
        store.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, true));
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
    validateDeletedColumns(catalog.id(), catalog.type());

    Assertions.assertThrowsExactly(
        NoSuchEntityException.class,
        () -> store.get(schema2.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class));
    // Delete again should return false
    Assertions.assertFalse(store.delete(catalog.nameIdentifier(), Entity.EntityType.CATALOG, true));
  }

  private void validateDeleteSchemaCascade(
      EntityStore store,
      SchemaEntity schema1,
      TableEntity table1,
      FilesetEntity fileset1,
      TopicEntity topic1,
      ModelEntity model1)
      throws IOException {
    TableEntity table1New =
        createTableEntityWithColumns(
            RandomIdGenerator.INSTANCE.nextId(),
            table1.namespace(),
            table1.name(),
            table1.auditInfo(),
            table1.columns());
    store.put(table1New);
    FilesetEntity fileset1New =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            fileset1.namespace(),
            fileset1.name(),
            fileset1.auditInfo());
    store.put(fileset1New);
    TopicEntity topic1New =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            topic1.namespace(),
            topic1.name(),
            topic1.auditInfo());
    store.put(topic1New);

    ModelEntity model1New =
        TestJDBCBackend.createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            model1.namespace(),
            model1.name(),
            model1.comment(),
            model1.latestVersion(),
            model1.properties(),
            model1.auditInfo());
    store.put(model1New);

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

    validateDeletedColumns(schema1.id(), schema1.type());

    // Delete again should return false
    Assertions.assertFalse(store.delete(schema1.nameIdentifier(), Entity.EntityType.SCHEMA, true));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> store.get(fileset1.nameIdentifier(), Entity.EntityType.FILESET, FilesetEntity.class));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> store.get(topic1.nameIdentifier(), Entity.EntityType.TOPIC, TopicEntity.class));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> store.get(model1.nameIdentifier(), Entity.EntityType.MODEL, ModelEntity.class));
  }

  private void validateDeleteMetalake(
      EntityStore store,
      BaseMetalake metalake,
      CatalogEntity catalogCopy,
      UserEntity user2,
      GroupEntity group2,
      RoleEntity role2)
      throws IOException {
    // Now delete catalog 'catalogCopy' and metalake
    Assertions.assertTrue(store.exists(user2.nameIdentifier(), Entity.EntityType.USER));
    Assertions.assertTrue(store.exists(group2.nameIdentifier(), Entity.EntityType.GROUP));
    Assertions.assertTrue(store.exists(role2.nameIdentifier(), Entity.EntityType.ROLE));

    Assertions.assertThrowsExactly(
        NonEmptyEntityException.class,
        () -> store.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
    store.delete(catalogCopy.nameIdentifier(), Entity.EntityType.CATALOG);
    Assertions.assertFalse(store.exists(catalogCopy.nameIdentifier(), Entity.EntityType.CATALOG));

    store.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE);
    Assertions.assertFalse(store.exists(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
    Assertions.assertFalse(store.exists(user2.nameIdentifier(), Entity.EntityType.USER));
    Assertions.assertFalse(store.exists(group2.nameIdentifier(), Entity.EntityType.GROUP));
    Assertions.assertFalse(store.exists(role2.nameIdentifier(), Entity.EntityType.ROLE));
    // Delete again should return false
    Assertions.assertFalse(store.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
  }

  private void validateDeleteCatalog(
      EntityStore store,
      CatalogEntity catalog,
      TableEntity table1,
      SchemaEntity schema1,
      TableEntity table1InSchema2,
      SchemaEntity schema2,
      FilesetEntity fileset1,
      FilesetEntity fileset1InSchema2,
      TopicEntity topic1,
      TopicEntity topic1InSchema2,
      ModelEntity model1,
      ModelEntity model1InSchema2)
      throws IOException {
    // Now try to delete all schemas under catalog;
    Assertions.assertThrowsExactly(
        NonEmptyEntityException.class,
        () -> store.delete(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    store.delete(table1.nameIdentifier(), Entity.EntityType.TABLE);
    validateDeletedColumns(table1.id(), table1.type());
    store.delete(fileset1.nameIdentifier(), Entity.EntityType.FILESET);
    store.delete(topic1.nameIdentifier(), Entity.EntityType.TOPIC);
    store.delete(model1.nameIdentifier(), Entity.EntityType.MODEL);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    store.delete(schema1.nameIdentifier(), Entity.EntityType.SCHEMA);
    store.delete(table1InSchema2.nameIdentifier(), Entity.EntityType.TABLE);
    validateDeletedColumns(table1InSchema2.id(), table1InSchema2.type());
    Assertions.assertFalse(
        store.exists(fileset1InSchema2.nameIdentifier(), Entity.EntityType.FILESET));
    Assertions.assertFalse(store.exists(topic1InSchema2.nameIdentifier(), Entity.EntityType.TOPIC));
    Assertions.assertFalse(store.exists(model1InSchema2.nameIdentifier(), Entity.EntityType.MODEL));
    store.delete(schema2.nameIdentifier(), Entity.EntityType.SCHEMA);

    store.delete(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    Assertions.assertFalse(store.exists(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    // Delete again should return false
    Assertions.assertFalse(store.delete(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
  }

  private void validateDeleteSchema(
      EntityStore store,
      SchemaEntity schema1,
      TableEntity table1,
      FilesetEntity fileset1,
      TopicEntity topic1,
      ModelEntity model1,
      ModelVersionEntity modelVersion1)
      throws IOException {
    // Delete the schema 'metalake.catalog.schema1' but failed, because it ha sub-entities;
    NonEmptyEntityException exception =
        Assertions.assertThrowsExactly(
            NonEmptyEntityException.class,
            () -> store.delete(schema1.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(exception.getMessage().contains("metalake.catalog.schema1"));
    // Make sure schema 'metalake.catalog.schema1', table 'metalake.catalog.schema1.table1',
    // table 'metalake.catalog.schema1.fileset1' and table 'metalake.catalog.schema1.topic1'
    // has not been deleted yet;
    Assertions.assertTrue(store.exists(schema1.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(store.exists(table1.nameIdentifier(), Entity.EntityType.TABLE));

    Assertions.assertTrue(store.exists(fileset1.nameIdentifier(), Entity.EntityType.FILESET));
    Assertions.assertTrue(store.exists(topic1.nameIdentifier(), Entity.EntityType.TOPIC));
    Assertions.assertTrue(store.exists(model1.nameIdentifier(), Entity.EntityType.MODEL));
    Assertions.assertTrue(
        store.exists(modelVersion1.nameIdentifier(), Entity.EntityType.MODEL_VERSION));

    // Delete table1,fileset1 and schema1
    Assertions.assertTrue(store.delete(table1.nameIdentifier(), Entity.EntityType.TABLE));
    validateDeletedColumns(table1.id(), table1.type());
    Assertions.assertTrue(store.delete(fileset1.nameIdentifier(), Entity.EntityType.FILESET));
    Assertions.assertTrue(store.delete(topic1.nameIdentifier(), Entity.EntityType.TOPIC));
    Assertions.assertTrue(store.delete(model1.nameIdentifier(), Entity.EntityType.MODEL));
    Assertions.assertTrue(store.delete(schema1.nameIdentifier(), Entity.EntityType.SCHEMA));
    // Make sure table1, fileset1 in 'metalake.catalog.schema1' can't be access;
    Assertions.assertFalse(store.exists(table1.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertFalse(store.exists(fileset1.nameIdentifier(), Entity.EntityType.FILESET));
    Assertions.assertFalse(store.exists(topic1.nameIdentifier(), Entity.EntityType.TOPIC));
    Assertions.assertFalse(store.exists(model1.nameIdentifier(), Entity.EntityType.MODEL));
    Assertions.assertFalse(
        store.exists(modelVersion1.nameIdentifier(), Entity.EntityType.MODEL_VERSION));
    Assertions.assertFalse(store.exists(schema1.nameIdentifier(), Entity.EntityType.SCHEMA));

    // Delete again should return false
    Assertions.assertFalse(store.delete(table1.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertFalse(store.delete(fileset1.nameIdentifier(), Entity.EntityType.FILESET));
    Assertions.assertFalse(store.delete(topic1.nameIdentifier(), Entity.EntityType.TOPIC));
    Assertions.assertFalse(store.delete(model1.nameIdentifier(), Entity.EntityType.MODEL));
    Assertions.assertFalse(store.delete(schema1.nameIdentifier(), Entity.EntityType.SCHEMA));

    // Now we re-insert schema1, table1, fileset1 and topic1, and everything should be OK
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
    TopicEntity topic1New =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            topic1.namespace(),
            topic1.name(),
            topic1.auditInfo());
    store.put(topic1New);

    ModelEntity model1New =
        TestJDBCBackend.createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            model1.namespace(),
            model1.name(),
            model1.comment(),
            model1.latestVersion(),
            model1.properties(),
            model1.auditInfo());
    store.put(model1New);

    Assertions.assertEquals(
        schema1New,
        store.get(schema1.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class));
    Assertions.assertEquals(
        table1New, store.get(table1.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class));
    Assertions.assertEquals(
        fileset1New,
        store.get(fileset1.nameIdentifier(), Entity.EntityType.FILESET, FilesetEntity.class));
    Assertions.assertEquals(
        topic1New, store.get(topic1.nameIdentifier(), Entity.EntityType.TOPIC, TopicEntity.class));
    Assertions.assertEquals(
        model1New, store.get(model1.nameIdentifier(), Entity.EntityType.MODEL, ModelEntity.class));
  }

  private void validateDeleteUser(EntityStore store, UserEntity user1) throws IOException {
    Assertions.assertTrue(store.exists(user1.nameIdentifier(), Entity.EntityType.USER));
    Assertions.assertTrue(store.delete(user1.nameIdentifier(), Entity.EntityType.USER));
    Assertions.assertFalse(store.exists(user1.nameIdentifier(), Entity.EntityType.USER));
    // delete again should return false
    Assertions.assertFalse(store.delete(user1.nameIdentifier(), Entity.EntityType.USER));

    UserEntity user =
        createUser(RandomIdGenerator.INSTANCE.nextId(), "metalake", "user1", user1.auditInfo());
    store.put(user);
    Assertions.assertTrue(store.exists(user.nameIdentifier(), Entity.EntityType.USER));
  }

  private void validateDeleteGroup(EntityStore store, GroupEntity group1) throws IOException {
    Assertions.assertTrue(store.delete(group1.nameIdentifier(), EntityType.GROUP));
    Assertions.assertFalse(store.exists(group1.nameIdentifier(), Entity.EntityType.GROUP));
    // delete again should return false
    Assertions.assertFalse(store.delete(group1.nameIdentifier(), Entity.EntityType.GROUP));

    GroupEntity group =
        createGroup(RandomIdGenerator.INSTANCE.nextId(), "metalake", "group1", group1.auditInfo());
    store.put(group);
    Assertions.assertTrue(store.exists(group.nameIdentifier(), EntityType.GROUP));
  }

  private void validateDeleteRole(EntityStore store, RoleEntity role1) throws IOException {
    Assertions.assertTrue(store.delete(role1.nameIdentifier(), EntityType.ROLE));
    Assertions.assertFalse(store.exists(role1.nameIdentifier(), Entity.EntityType.ROLE));
    // delete again should return false
    Assertions.assertFalse(store.delete(role1.nameIdentifier(), Entity.EntityType.ROLE));

    RoleEntity role =
        createRole(RandomIdGenerator.INSTANCE.nextId(), "metalake", "role1", role1.auditInfo());
    store.put(role);
    Assertions.assertTrue(store.exists(role.nameIdentifier(), EntityType.ROLE));
  }

  private void validateDeleteTable(
      EntityStore store, SchemaEntity schema2, TableEntity table1, TableEntity table1InSchema2)
      throws IOException {
    // Delete the table 'metalake.catalog.schema2.table1'
    Assertions.assertTrue(store.delete(table1InSchema2.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertFalse(store.exists(table1InSchema2.nameIdentifier(), Entity.EntityType.TABLE));
    // delete again should return false
    Assertions.assertFalse(store.delete(table1InSchema2.nameIdentifier(), Entity.EntityType.TABLE));

    // Make sure all columns are deleted
    validateDeletedColumns(table1InSchema2.id(), table1InSchema2.type());

    // Make sure table 'metalake.catalog.schema1.table1' still exist;
    Assertions.assertEquals(
        table1, store.get(table1.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class));
    // Make sure schema 'metalake.catalog.schema2' still exist;
    Assertions.assertEquals(
        schema2, store.get(schema2.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class));
    // Re-insert table1InSchema2 and everything is OK
    TableEntity table1InSchema2New =
        createTableEntityWithColumns(
            RandomIdGenerator.INSTANCE.nextId(),
            table1InSchema2.namespace(),
            table1InSchema2.name(),
            table1InSchema2.auditInfo(),
            table1InSchema2.columns());
    store.put(table1InSchema2New);
    Assertions.assertTrue(store.exists(table1InSchema2.nameIdentifier(), Entity.EntityType.TABLE));
  }

  private void validateDeleteModel(
      EntityStore store,
      SchemaEntity schema2,
      ModelEntity model1,
      ModelVersionEntity modelVersion1,
      ModelEntity model1InSchema2,
      ModelVersionEntity modelVersion1InSchema2)
      throws IOException {
    Assertions.assertTrue(store.delete(model1InSchema2.nameIdentifier(), Entity.EntityType.MODEL));
    Assertions.assertFalse(store.exists(model1InSchema2.nameIdentifier(), Entity.EntityType.MODEL));
    // delete again should return false
    Assertions.assertFalse(store.delete(model1InSchema2.nameIdentifier(), Entity.EntityType.MODEL));

    Assertions.assertFalse(
        store.exists(modelVersion1InSchema2.nameIdentifier(), EntityType.MODEL_VERSION));
    Assertions.assertFalse(
        store.delete(modelVersion1InSchema2.nameIdentifier(), EntityType.MODEL_VERSION));

    ModelEntity model1Copy =
        ModelEntity.builder()
            .withId(model1.id())
            .withNamespace(model1.namespace())
            .withName(model1.name())
            .withComment(model1.comment())
            .withLatestVersion(model1.latestVersion() + 1)
            .withProperties(model1.properties())
            .withAuditInfo(model1.auditInfo())
            .build();

    Assertions.assertEquals(
        model1Copy, store.get(model1.nameIdentifier(), Entity.EntityType.MODEL, ModelEntity.class));

    Assertions.assertEquals(
        modelVersion1,
        store.get(
            modelVersion1.nameIdentifier(), EntityType.MODEL_VERSION, ModelVersionEntity.class));

    // Make sure schema 'metalake.catalog.schema2' still exist;
    Assertions.assertEquals(
        schema2, store.get(schema2.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class));
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
      FilesetEntity fileset1InSchema2,
      TopicEntity topic1,
      TopicEntity topic1InSchema2,
      ModelEntity model1,
      ModelVersionEntity modelVersion1,
      ModelEntity model1InSchema2,
      ModelVersionEntity modelVersion1InSchema2,
      UserEntity user1,
      UserEntity user2,
      GroupEntity group1,
      GroupEntity group2,
      RoleEntity role1,
      RoleEntity role2)
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
        fileset1InSchema2,
        store.get(
            fileset1InSchema2.nameIdentifier(), Entity.EntityType.FILESET, FilesetEntity.class));
    Assertions.assertEquals(
        topic1, store.get(topic1.nameIdentifier(), Entity.EntityType.TOPIC, TopicEntity.class));
    Assertions.assertEquals(
        topic1InSchema2,
        store.get(topic1InSchema2.nameIdentifier(), Entity.EntityType.TOPIC, TopicEntity.class));

    ModelEntity model1Copy =
        ModelEntity.builder()
            .withId(model1.id())
            .withNamespace(model1.namespace())
            .withName(model1.name())
            .withComment(model1.comment())
            .withLatestVersion(model1.latestVersion() + 1)
            .withProperties(model1.properties())
            .withAuditInfo(model1.auditInfo())
            .build();

    Assertions.assertEquals(
        model1Copy, store.get(model1.nameIdentifier(), Entity.EntityType.MODEL, ModelEntity.class));
    Assertions.assertEquals(
        modelVersion1,
        store.get(
            modelVersion1.nameIdentifier(), EntityType.MODEL_VERSION, ModelVersionEntity.class));

    ModelEntity model1InSchema2Copy =
        ModelEntity.builder()
            .withId(model1InSchema2.id())
            .withNamespace(model1InSchema2.namespace())
            .withName(model1InSchema2.name())
            .withComment(model1InSchema2.comment())
            .withLatestVersion(model1InSchema2.latestVersion() + 1)
            .withProperties(model1InSchema2.properties())
            .withAuditInfo(model1InSchema2.auditInfo())
            .build();

    Assertions.assertEquals(
        model1InSchema2Copy,
        store.get(model1InSchema2.nameIdentifier(), EntityType.MODEL, ModelEntity.class));
    Assertions.assertEquals(
        modelVersion1InSchema2,
        store.get(
            modelVersion1InSchema2.nameIdentifier(),
            EntityType.MODEL_VERSION,
            ModelVersionEntity.class));

    Assertions.assertEquals(
        user1, store.get(user1.nameIdentifier(), Entity.EntityType.USER, UserEntity.class));
    Assertions.assertEquals(
        user2, store.get(user2.nameIdentifier(), Entity.EntityType.USER, UserEntity.class));
    Assertions.assertEquals(
        group1, store.get(group1.nameIdentifier(), Entity.EntityType.GROUP, GroupEntity.class));
    Assertions.assertEquals(
        group2, store.get(group2.nameIdentifier(), Entity.EntityType.GROUP, GroupEntity.class));
    Assertions.assertEquals(
        role1, store.get(role1.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class));
    Assertions.assertEquals(
        role2, store.get(role2.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class));
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

  private void validateDeletedTopic(EntityStore store) throws IOException {
    store.delete(
        NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "topic1"),
        Entity.EntityType.TOPIC);
    // Update a deleted entities
    Assertions.assertThrowsExactly(
        NoSuchEntityException.class,
        () ->
            store.update(
                NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "topic1"),
                TopicEntity.class,
                Entity.EntityType.TOPIC,
                (e) -> e));
  }

  private void validateTopicChanged(EntityStore store, TopicEntity topicEntity) throws IOException {
    // Check topic entities
    store.update(
        NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged", "topic1"),
        TopicEntity.class,
        Entity.EntityType.TOPIC,
        e -> {
          AuditInfo auditInfo1 =
              AuditInfo.builder().withCreator("creator6").withCreateTime(Instant.now()).build();
          return createTopicEntity(
              topicEntity.id(),
              Namespace.of("metalakeChanged", "catalogChanged", "schemaChanged"),
              "topicChanged",
              auditInfo1);
        });

    Assertions.assertThrowsExactly(
        NoSuchEntityException.class,
        () ->
            store.get(
                NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1", "topic1"),
                Entity.EntityType.TOPIC,
                TopicEntity.class));
    TopicEntity updatedTopic =
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged", "topicChanged"),
            Entity.EntityType.TOPIC,
            TopicEntity.class);
    Assertions.assertEquals("creator6", updatedTopic.auditInfo().creator());

    Assertions.assertNotNull(
        store.get(
            NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "topic1"),
            Entity.EntityType.TOPIC,
            TopicEntity.class));
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
        EntityAlreadyExistsException.class,
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

  private List<Pair<Long, Pair<Long, Long>>> listAllColumnWithEntityId(
      Long entityId, Entity.EntityType entityType) {
    String queryTemp =
        "SELECT column_id, table_version, deleted_at FROM "
            + "table_column_version_info WHERE %s = %d";
    String query;
    switch (entityType) {
      case TABLE:
        query = String.format(queryTemp, "table_id", entityId);
        break;
      case SCHEMA:
        query = String.format(queryTemp, "schema_id", entityId);
        break;
      case CATALOG:
        query = String.format(queryTemp, "catalog_id", entityId);
        break;
      case METALAKE:
        query = String.format(queryTemp, "metalake_id", entityId);
        break;
      default:
        throw new IllegalArgumentException("Unsupported entity type: " + entityType);
    }

    List<Pair<Long, Pair<Long, Long>>> results = Lists.newArrayList();
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      Connection connection = sqlSession.getConnection();
      Statement statement = connection.createStatement();

      ResultSet rs = statement.executeQuery(query);
      while (rs.next()) {
        results.add(
            Pair.of(
                rs.getLong("column_id"),
                Pair.of(rs.getLong("table_version"), rs.getLong("deleted_at"))));
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return results;
  }

  private void validateDeletedColumns(Long entityId, Entity.EntityType entityType) {
    List<Pair<Long, Pair<Long, Long>>> deleteResult =
        listAllColumnWithEntityId(entityId, entityType);
    deleteResult.forEach(p -> Assertions.assertTrue(p.getRight().getRight() > 0));
  }
}
