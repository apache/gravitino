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
package org.apache.gravitino.storage.relational.service;

import java.io.IOException;
import java.time.Instant;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestOwnerMetaService extends TestJDBCBackend {

  String metalakeName = "metalake";

  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

  @Test
  void testDifferentOwners() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);
    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user",
            auditInfo);
    backend.insert(user, false);
    GroupEntity group =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group",
            auditInfo);
    backend.insert(group, false);

    // Test no owner
    Assertions.assertFalse(
        OwnerMetaService.getInstance()
            .getOwner(metalake.nameIdentifier(), metalake.type())
            .isPresent());

    // Test a user owner
    OwnerMetaService.getInstance()
        .setOwner(metalake.nameIdentifier(), metalake.type(), user.nameIdentifier(), user.type());

    Entity entity =
        OwnerMetaService.getInstance().getOwner(metalake.nameIdentifier(), metalake.type()).get();
    Assertions.assertTrue(entity instanceof UserEntity);
    Assertions.assertEquals("user", ((UserEntity) entity).name());

    // Test a group owner
    OwnerMetaService.getInstance()
        .setOwner(metalake.nameIdentifier(), metalake.type(), group.nameIdentifier(), group.type());

    entity =
        OwnerMetaService.getInstance().getOwner(metalake.nameIdentifier(), metalake.type()).get();
    Assertions.assertTrue(entity instanceof GroupEntity);
    Assertions.assertEquals("group", ((GroupEntity) entity).name());
  }

  @Test
  void testDifferentEntities() throws IOException {
    String catalogName = "catalog";
    String schemaName = "schema";
    String tableName = "table";
    String filesetName = "fileset";
    String topicName = "topic";
    String userName = "user";
    String groupName = "group";
    String roleName = "role";

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
            Namespace.of(metalakeName, catalogName),
            schemaName,
            auditInfo);
    backend.insert(schema, false);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalogName, schemaName),
            tableName,
            auditInfo);
    backend.insert(table, false);

    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalogName, schemaName),
            topicName,
            auditInfo);
    backend.insert(topic, false);

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalogName, schemaName),
            filesetName,
            auditInfo);
    backend.insert(fileset, false);

    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            userName,
            auditInfo);
    backend.insert(user, false);

    GroupEntity group =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            groupName,
            auditInfo);
    backend.insert(group, false);

    RoleEntity role =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            roleName,
            auditInfo,
            catalogName);
    backend.insert(role, false);

    OwnerMetaService.getInstance()
        .setOwner(metalake.nameIdentifier(), metalake.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(catalog.nameIdentifier(), catalog.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(schema.nameIdentifier(), schema.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(fileset.nameIdentifier(), fileset.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(table.nameIdentifier(), table.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(topic.nameIdentifier(), topic.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(role.nameIdentifier(), role.type(), user.nameIdentifier(), user.type());

    Entity entity =
        OwnerMetaService.getInstance().getOwner(metalake.nameIdentifier(), metalake.type()).get();
    Assertions.assertTrue(entity instanceof UserEntity);
    Assertions.assertEquals("user", ((UserEntity) entity).name());

    entity =
        OwnerMetaService.getInstance().getOwner(catalog.nameIdentifier(), catalog.type()).get();
    Assertions.assertTrue(entity instanceof UserEntity);
    Assertions.assertEquals("user", ((UserEntity) entity).name());

    entity = OwnerMetaService.getInstance().getOwner(schema.nameIdentifier(), schema.type()).get();
    Assertions.assertTrue(entity instanceof UserEntity);
    Assertions.assertEquals("user", ((UserEntity) entity).name());

    entity = OwnerMetaService.getInstance().getOwner(table.nameIdentifier(), table.type()).get();
    Assertions.assertTrue(entity instanceof UserEntity);
    Assertions.assertEquals("user", ((UserEntity) entity).name());

    entity = OwnerMetaService.getInstance().getOwner(topic.nameIdentifier(), topic.type()).get();
    Assertions.assertTrue(entity instanceof UserEntity);
    Assertions.assertEquals("user", ((UserEntity) entity).name());

    entity =
        OwnerMetaService.getInstance().getOwner(fileset.nameIdentifier(), fileset.type()).get();
    Assertions.assertTrue(entity instanceof UserEntity);
    Assertions.assertEquals("user", ((UserEntity) entity).name());

    entity = OwnerMetaService.getInstance().getOwner(role.nameIdentifier(), role.type()).get();
    Assertions.assertTrue(entity instanceof UserEntity);
    Assertions.assertEquals("user", ((UserEntity) entity).name());
  }

  @Test
  public void testDeleteMetadataObject() throws IOException {
    String metalakeName = "metalake";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    backend.insert(fileset, false);
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);
    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    backend.insert(topic, false);
    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "model",
            "comment",
            1,
            null,
            auditInfo);
    backend.insert(model, false);

    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user",
            auditInfo);
    backend.insert(user, false);

    OwnerMetaService.getInstance()
        .setOwner(catalog.nameIdentifier(), catalog.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(schema.nameIdentifier(), schema.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(table.nameIdentifier(), table.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(fileset.nameIdentifier(), fileset.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(topic.nameIdentifier(), topic.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(model.nameIdentifier(), model.type(), user.nameIdentifier(), user.type());

    Assertions.assertEquals(6, countAllOwnerRel(user.id()));
    Assertions.assertEquals(6, countActiveOwnerRel(user.id()));

    // Test to delete model
    ModelMetaService.getInstance().deleteModel(model.nameIdentifier());
    Assertions.assertEquals(6, countAllOwnerRel(user.id()));
    Assertions.assertEquals(5, countActiveOwnerRel(user.id()));

    // Test to delete table
    TableMetaService.getInstance().deleteTable(table.nameIdentifier());
    Assertions.assertEquals(6, countAllOwnerRel(user.id()));
    Assertions.assertEquals(4, countActiveOwnerRel(user.id()));

    // Test to delete topic
    TopicMetaService.getInstance().deleteTopic(topic.nameIdentifier());
    Assertions.assertEquals(6, countAllOwnerRel(user.id()));
    Assertions.assertEquals(3, countActiveOwnerRel(user.id()));

    // Test to delete fileset
    FilesetMetaService.getInstance().deleteFileset(fileset.nameIdentifier());
    Assertions.assertEquals(6, countAllOwnerRel(user.id()));
    Assertions.assertEquals(2, countActiveOwnerRel(user.id()));

    // Test to delete schema
    SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), false);
    Assertions.assertEquals(6, countAllOwnerRel(user.id()));
    Assertions.assertEquals(1, countActiveOwnerRel(user.id()));

    // Test to delete catalog
    CatalogMetaService.getInstance().deleteCatalog(catalog.nameIdentifier(), false);
    Assertions.assertEquals(6, countAllOwnerRel(user.id()));
    Assertions.assertEquals(0, countActiveOwnerRel(user.id()));

    // Test to delete catalog with cascade mode
    catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    backend.insert(fileset, false);
    table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);
    topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    backend.insert(topic, false);
    model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "model",
            "comment",
            1,
            null,
            auditInfo);
    backend.insert(model, false);

    OwnerMetaService.getInstance()
        .setOwner(catalog.nameIdentifier(), catalog.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(schema.nameIdentifier(), schema.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(table.nameIdentifier(), table.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(fileset.nameIdentifier(), fileset.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(topic.nameIdentifier(), topic.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(model.nameIdentifier(), model.type(), user.nameIdentifier(), user.type());

    CatalogMetaService.getInstance().deleteCatalog(catalog.nameIdentifier(), true);
    Assertions.assertEquals(12, countAllOwnerRel(user.id()));
    Assertions.assertEquals(0, countActiveOwnerRel(user.id()));

    // Test to delete schema with cascade mode
    catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    backend.insert(fileset, false);

    table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);

    topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    backend.insert(topic, false);

    model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "model",
            "comment",
            1,
            null,
            auditInfo);
    backend.insert(model, false);

    OwnerMetaService.getInstance()
        .setOwner(schema.nameIdentifier(), schema.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(catalog.nameIdentifier(), catalog.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(table.nameIdentifier(), table.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(fileset.nameIdentifier(), fileset.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(topic.nameIdentifier(), topic.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(model.nameIdentifier(), model.type(), user.nameIdentifier(), user.type());

    SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), true);
    Assertions.assertEquals(18, countAllOwnerRel(user.id()));
    Assertions.assertEquals(1, countActiveOwnerRel(user.id()));

    // Test to delete user
    schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    backend.insert(fileset, false);
    table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);
    topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "topic",
            auditInfo);

    backend.insert(topic, false);

    model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "model",
            "comment",
            1,
            null,
            auditInfo);
    backend.insert(model, false);

    OwnerMetaService.getInstance()
        .setOwner(schema.nameIdentifier(), schema.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(catalog.nameIdentifier(), catalog.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(table.nameIdentifier(), table.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(fileset.nameIdentifier(), fileset.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(topic.nameIdentifier(), topic.type(), user.nameIdentifier(), user.type());
    OwnerMetaService.getInstance()
        .setOwner(model.nameIdentifier(), model.type(), user.nameIdentifier(), user.type());

    UserMetaService.getInstance().deleteUser(user.nameIdentifier());
    Assertions.assertEquals(24, countAllOwnerRel(user.id()));
    Assertions.assertEquals(0, countActiveOwnerRel(user.id()));
  }
}
