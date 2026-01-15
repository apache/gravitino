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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.exceptions.NoSuchEntityException;
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
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

// Note: Do not add more tests in this class as it's already getting too large. If needed, please
// just extend AbstractEntityStoreTest and add a new test class. More, please refer to
// `TestEntityStoreForLance` or `TestEntityStoreRelationCache`.
@Tag("gravitino-docker-test")
public class TestEntityStorage extends AbstractEntityStorageTest {

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testRestart(String type, boolean enableCache) throws IOException, IllegalAccessException {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);

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
          createModelEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema1"),
              "model1",
              "model1",
              0,
              null,
              auditInfo);
      ModelVersionEntity modelVersion1 =
          createModelVersionEntity(
              model1.nameIdentifier(),
              0,
              ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"),
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
                  Entity.EntityType.MODEL_VERSION,
                  ModelVersionEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "model1", "alias2"),
                  Entity.EntityType.MODEL_VERSION,
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
    }

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
                  Entity.EntityType.MODEL_VERSION,
                  ModelVersionEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake", "catalog", "schema1", "model1", "alias2"),
                  Entity.EntityType.MODEL_VERSION,
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
  public void testAuthorizationEntityDelete(String type, boolean enableCache)
      throws IOException, IllegalAccessException {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
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
  void testEntityDelete(String type, boolean enableCache) throws IOException, IllegalAccessException {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
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
          createModelEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema1"),
              "model1",
              "model1",
              0,
              null,
              auditInfo);
      ModelVersionEntity modelVersion1 =
          createModelVersionEntity(
              model1.nameIdentifier(),
              0,
              ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"),
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
          createModelEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema2"),
              "model1",
              "model1",
              0,
              null,
              auditInfo);
      ModelVersionEntity modelVersion1InSchema2 =
          createModelVersionEntity(
              model1InSchema2.nameIdentifier(),
              0,
              ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"),
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

      BaseMetalake metalakeNew =
          createBaseMakeLake(
              RandomIdGenerator.INSTANCE.nextId(), metalake.name(), metalake.auditInfo());
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

      ModelEntity model1New =
          createModelEntity(
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
  void testEntityUpdate(String type, boolean enableCache) throws Exception, IllegalAccessException {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
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

      store.put(metalake);
      store.put(catalog);
      store.put(catalogCopy);
      store.put(schema1);
      store.put(table1);
      store.put(fileset1);
      store.put(topic1);
      store.put(schema2);
      store.put(table1InSchema2);
      store.put(fileset1InSchema2);
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
  void testDeleteAndRename(String type, boolean enableCache)
      throws IOException, IllegalAccessException {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
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

      BaseMetalake metalake1New =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake1", auditInfo);
      store.put(metalake1New);
      store.update(
          NameIdentifier.of("metalake1"),
          BaseMetalake.class,
          Entity.EntityType.METALAKE,
          e -> createBaseMakeLake(metalake1New.id(), "metalake2", e.auditInfo()));

      BaseMetalake metalake3New1 =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake3", auditInfo);
      store.put(metalake3New1);
      store.update(
          NameIdentifier.of("metalake3"),
          BaseMetalake.class,
          Entity.EntityType.METALAKE,
          e -> createBaseMakeLake(metalake3New1.id(), "metalake1", e.auditInfo()));

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

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSameNameUnderANameSpace(String type, boolean enableCache)
      throws IOException, IllegalAccessException {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
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
          createModelEntity(
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

      Assertions.assertTrue(store.delete(identifier, Entity.EntityType.TABLE));
      Assertions.assertNotNull(
          store.get(identifier, Entity.EntityType.FILESET, FilesetEntity.class));
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.TOPIC, TopicEntity.class));
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.MODEL, ModelEntity.class));

      table1 =
          createTableEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, "sameName", auditInfo);
      store.put(table1);

      store.delete(identifier, Entity.EntityType.FILESET);
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.TABLE, TableEntity.class));
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.TOPIC, TopicEntity.class));
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.MODEL, ModelEntity.class));

      filesetEntity1 =
          createFilesetEntity(
              RandomIdGenerator.INSTANCE.nextId(), namespace, "sameName", auditInfo);
      store.put(filesetEntity1);

      store.delete(identifier, Entity.EntityType.TOPIC);
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.TABLE, TableEntity.class));
      Assertions.assertNotNull(
          store.get(identifier, Entity.EntityType.FILESET, FilesetEntity.class));
      Assertions.assertNotNull(store.get(identifier, Entity.EntityType.MODEL, ModelEntity.class));

      topicEntity1 =
          createTopicEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, "sameName", auditInfo);
      store.put(topicEntity1);

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
  // Note: Do not add more tests in this class as it's already getting too large. If needed, please
  // just extend AbstractEntityStoreTest and add a new test class. More, please refer to
  // `TestEntityStoreForLance` or `TestEntityStoreRelationCache`.
}
