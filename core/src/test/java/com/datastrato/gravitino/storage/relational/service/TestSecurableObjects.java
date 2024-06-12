/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.authorization.AuthorizationUtils;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.meta.TopicEntity;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.relational.TestJDBCBackend;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecurableObjects extends TestJDBCBackend {
  RoleMetaService roleMetaService = RoleMetaService.getInstance();

  @Test
  public void testAllTypeSecurableObjects() {
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
            Lists.newArrayList(Privileges.UseCatalog.allow(), Privileges.DropCatalog.deny()));

    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, "schema", Lists.newArrayList(Privileges.UseSchema.allow()));
    SecurableObject tableObject =
        SecurableObjects.ofTable(
            schemaObject, "table", Lists.newArrayList(Privileges.ReadTable.allow()));
    SecurableObject filesetObject =
        SecurableObjects.ofFileset(
            schemaObject, "fileset", Lists.newArrayList(Privileges.ReadFileset.allow()));
    SecurableObject topicObject =
        SecurableObjects.ofTopic(
            schemaObject, "topic", Lists.newArrayList(Privileges.ReadTopic.deny()));
    SecurableObject allMetalakesObject =
        SecurableObjects.ofAllMetalakes(Lists.newArrayList(Privileges.UseMetalake.allow()));

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo,
            Lists.newArrayList(
                catalogObject,
                schemaObject,
                tableObject,
                filesetObject,
                topicObject,
                allMetalakesObject),
            ImmutableMap.of("k1", "v1"));

    Assertions.assertDoesNotThrow(() -> roleMetaService.insertRole(role1, false));
    Assertions.assertEquals(role1, roleMetaService.getRoleByIdentifier(role1.nameIdentifier()));
  }
}
