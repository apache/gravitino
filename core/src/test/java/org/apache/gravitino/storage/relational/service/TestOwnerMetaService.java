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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestOwnerMetaService extends TestJDBCBackend {

  private static final String METALAKE_NAME = "metalake_for_owner_test";
  private static final String CATALOG_NAME = "catalog_for_owner_test";
  private static final String SCHEMA_NAME = "schema_for_owner_test";
  private static final String TABLE_NAME = "table_for_owner_test";
  private static final String FILESET_NAME = "fileset_for_owner_test";
  private static final String TOPIC_NAME = "topic_for_owner_test";

  @TestTemplate
  void testDifferentOwners() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);

    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE_NAME),
            "user",
            AUDIT_INFO);
    backend.insert(user, false);
    GroupEntity group =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(METALAKE_NAME),
            "group",
            AUDIT_INFO);
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

  @TestTemplate
  void testDifferentEntities() throws IOException {
    String userName = "user";
    String groupName = "group";
    String roleName = "role";

    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    CatalogEntity catalog = createAndInsertCatalog(METALAKE_NAME, CATALOG_NAME);
    SchemaEntity schema = createAndInsertSchema(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            TABLE_NAME,
            AUDIT_INFO);
    backend.insert(table, false);

    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            TOPIC_NAME,
            AUDIT_INFO);
    backend.insert(topic, false);

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            FILESET_NAME,
            AUDIT_INFO);
    backend.insert(fileset, false);

    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE_NAME),
            userName,
            AUDIT_INFO);
    backend.insert(user, false);

    GroupEntity group =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(METALAKE_NAME),
            groupName,
            AUDIT_INFO);
    backend.insert(group, false);

    RoleEntity role =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE_NAME),
            roleName,
            AUDIT_INFO,
            CATALOG_NAME);
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
    Assertions.assertInstanceOf(UserEntity.class, entity);
    Assertions.assertEquals("user", ((UserEntity) entity).name());

    entity =
        OwnerMetaService.getInstance().getOwner(catalog.nameIdentifier(), catalog.type()).get();
    Assertions.assertInstanceOf(UserEntity.class, entity);
    Assertions.assertEquals("user", ((UserEntity) entity).name());

    entity = OwnerMetaService.getInstance().getOwner(schema.nameIdentifier(), schema.type()).get();
    Assertions.assertInstanceOf(UserEntity.class, entity);
    Assertions.assertEquals("user", ((UserEntity) entity).name());

    entity = OwnerMetaService.getInstance().getOwner(table.nameIdentifier(), table.type()).get();
    Assertions.assertInstanceOf(UserEntity.class, entity);
    Assertions.assertEquals("user", ((UserEntity) entity).name());

    entity = OwnerMetaService.getInstance().getOwner(topic.nameIdentifier(), topic.type()).get();
    Assertions.assertInstanceOf(UserEntity.class, entity);
    Assertions.assertEquals("user", ((UserEntity) entity).name());

    entity =
        OwnerMetaService.getInstance().getOwner(fileset.nameIdentifier(), fileset.type()).get();
    Assertions.assertInstanceOf(UserEntity.class, entity);
    Assertions.assertEquals("user", ((UserEntity) entity).name());

    entity = OwnerMetaService.getInstance().getOwner(role.nameIdentifier(), role.type()).get();
    Assertions.assertInstanceOf(UserEntity.class, entity);
    Assertions.assertEquals("user", ((UserEntity) entity).name());
  }

  @TestTemplate
  public void testDeleteMetadataObject() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    CatalogEntity catalog = createAndInsertCatalog(METALAKE_NAME, CATALOG_NAME);
    SchemaEntity schema = createAndInsertSchema(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME);

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "fileset",
            AUDIT_INFO);
    backend.insert(fileset, false);
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "table",
            AUDIT_INFO);
    backend.insert(table, false);
    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "topic",
            AUDIT_INFO);
    backend.insert(topic, false);
    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "model",
            "comment",
            1,
            null,
            AUDIT_INFO);
    backend.insert(model, false);

    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE_NAME),
            "user",
            AUDIT_INFO);
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
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME),
            CATALOG_NAME,
            AUDIT_INFO);
    backend.insert(catalog, false);

    schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME),
            SCHEMA_NAME,
            AUDIT_INFO);
    backend.insert(schema, false);

    fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "fileset",
            AUDIT_INFO);
    backend.insert(fileset, false);
    table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "table",
            AUDIT_INFO);
    backend.insert(table, false);
    topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "topic",
            AUDIT_INFO);
    backend.insert(topic, false);
    model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "model",
            "comment",
            1,
            null,
            AUDIT_INFO);
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
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME),
            CATALOG_NAME,
            AUDIT_INFO);
    backend.insert(catalog, false);

    schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME),
            SCHEMA_NAME,
            AUDIT_INFO);
    backend.insert(schema, false);

    fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "fileset",
            AUDIT_INFO);
    backend.insert(fileset, false);

    table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "table",
            AUDIT_INFO);
    backend.insert(table, false);

    topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "topic",
            AUDIT_INFO);
    backend.insert(topic, false);

    model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "model",
            "comment",
            1,
            null,
            AUDIT_INFO);
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
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME),
            SCHEMA_NAME,
            AUDIT_INFO);
    backend.insert(schema, false);

    fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "fileset",
            AUDIT_INFO);
    backend.insert(fileset, false);
    table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "table",
            AUDIT_INFO);
    backend.insert(table, false);
    topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "topic",
            AUDIT_INFO);

    backend.insert(topic, false);

    model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(TestOwnerMetaService.METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "model",
            "comment",
            1,
            null,
            AUDIT_INFO);
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

  private Integer countAllOwnerRel(Long ownerId) {
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

  private GroupEntity createGroupEntity(
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
}
