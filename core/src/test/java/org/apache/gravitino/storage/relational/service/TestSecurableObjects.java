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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecurableObjects extends TestJDBCBackend {
  RoleMetaService roleMetaService = RoleMetaService.getInstance();

  @Test
  public void testAllTypeSecurableObjects() throws IOException {
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

    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            "catalog",
            Lists.newArrayList(Privileges.UseCatalog.allow(), Privileges.CreateSchema.deny()));

    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, "schema", Lists.newArrayList(Privileges.UseSchema.allow()));
    SecurableObject tableObject =
        SecurableObjects.ofTable(
            schemaObject, "table", Lists.newArrayList(Privileges.SelectTable.allow()));
    SecurableObject filesetObject =
        SecurableObjects.ofFileset(
            schemaObject, "fileset", Lists.newArrayList(Privileges.ReadFileset.allow()));
    SecurableObject topicObject =
        SecurableObjects.ofTopic(
            schemaObject, "topic", Lists.newArrayList(Privileges.ConsumeTopic.deny()));

    ArrayList<SecurableObject> securableObjects =
        Lists.newArrayList(catalogObject, schemaObject, tableObject, filesetObject, topicObject);
    securableObjects.sort(Comparator.comparing(MetadataObject::fullName));

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            securableObjects,
            ImmutableMap.of("k1", "v1"));

    Assertions.assertDoesNotThrow(() -> roleMetaService.insertRole(role1, false));
    Assertions.assertEquals(role1, roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
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

    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            "catalog",
            Lists.newArrayList(Privileges.UseCatalog.allow(), Privileges.CreateSchema.deny()));

    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, "schema", Lists.newArrayList(Privileges.UseSchema.allow()));
    SecurableObject tableObject =
        SecurableObjects.ofTable(
            schemaObject, "table", Lists.newArrayList(Privileges.SelectTable.allow()));
    SecurableObject filesetObject =
        SecurableObjects.ofFileset(
            schemaObject, "fileset", Lists.newArrayList(Privileges.ReadFileset.allow()));
    SecurableObject topicObject =
        SecurableObjects.ofTopic(
            schemaObject, "topic", Lists.newArrayList(Privileges.ConsumeTopic.deny()));

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            Lists.newArrayList(
                catalogObject, schemaObject, tableObject, filesetObject, topicObject),
            ImmutableMap.of("k1", "v1"));

    roleMetaService.insertRole(role1, false);

    Assertions.assertEquals(5, countAllObjectRel(role1.id()));
    Assertions.assertEquals(5, countActiveObjectRel(role1.id()));

    // Test to delete table
    TableMetaService.getInstance().deleteTable(table.nameIdentifier());
    Assertions.assertEquals(5, countAllObjectRel(role1.id()));
    Assertions.assertEquals(4, countActiveObjectRel(role1.id()));

    // Test to delete topic
    TopicMetaService.getInstance().deleteTopic(topic.nameIdentifier());
    Assertions.assertEquals(5, countAllObjectRel(role1.id()));
    Assertions.assertEquals(3, countActiveObjectRel(role1.id()));

    // Test to delete fileset
    FilesetMetaService.getInstance().deleteFileset(fileset.nameIdentifier());
    Assertions.assertEquals(5, countAllObjectRel(role1.id()));
    Assertions.assertEquals(2, countActiveObjectRel(role1.id()));

    // Test to delete schema
    SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), false);
    Assertions.assertEquals(5, countAllObjectRel(role1.id()));
    Assertions.assertEquals(1, countActiveObjectRel(role1.id()));

    // Test to delete catalog
    CatalogMetaService.getInstance().deleteCatalog(catalog.nameIdentifier(), false);
    Assertions.assertEquals(5, countAllObjectRel(role1.id()));
    Assertions.assertEquals(0, countActiveObjectRel(role1.id()));

    roleMetaService.deleteRole(role1.nameIdentifier());

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
    role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            Lists.newArrayList(
                catalogObject, schemaObject, tableObject, filesetObject, topicObject),
            ImmutableMap.of("k1", "v1"));

    roleMetaService.insertRole(role1, false);

    CatalogMetaService.getInstance().deleteCatalog(catalog.nameIdentifier(), true);
    Assertions.assertEquals(5, countAllObjectRel(role1.id()));
    Assertions.assertEquals(0, countActiveObjectRel(role1.id()));

    roleMetaService.deleteRole(role1.nameIdentifier());

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
    role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            Lists.newArrayList(
                catalogObject, schemaObject, tableObject, filesetObject, topicObject),
            ImmutableMap.of("k1", "v1"));

    roleMetaService.insertRole(role1, false);

    SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), true);
    Assertions.assertEquals(5, countAllObjectRel(role1.id()));
    Assertions.assertEquals(1, countActiveObjectRel(role1.id()));
  }
}
