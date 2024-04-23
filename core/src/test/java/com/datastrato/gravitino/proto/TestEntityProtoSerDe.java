/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntitySerDe;
import com.datastrato.gravitino.EntitySerDeFactory;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.meta.GroupEntity;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.meta.UserEntity;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEntityProtoSerDe {

  private final EntitySerDe entitySerDe = EntitySerDeFactory.createEntitySerDe("proto");

  @Test
  public void testAuditInfoSerDe() throws IOException {
    Instant now = Instant.now();
    String creator = "creator";
    String modifier = "modifier";

    com.datastrato.gravitino.meta.AuditInfo auditInfo =
        com.datastrato.gravitino.meta.AuditInfo.builder()
            .withCreator(creator)
            .withCreateTime(now)
            .withLastModifier(modifier)
            .withLastModifiedTime(now)
            .build();

    ProtoEntitySerDe protoEntitySerDe = (ProtoEntitySerDe) entitySerDe;

    byte[] bytes = protoEntitySerDe.serialize(auditInfo);
    com.datastrato.gravitino.meta.AuditInfo auditInfoFromBytes =
        protoEntitySerDe.deserialize(
            bytes, com.datastrato.gravitino.meta.AuditInfo.class, Namespace.empty());
    Assertions.assertEquals(auditInfo, auditInfoFromBytes);

    // Test with optional fields
    com.datastrato.gravitino.meta.AuditInfo auditInfo1 =
        com.datastrato.gravitino.meta.AuditInfo.builder()
            .withCreator(creator)
            .withCreateTime(now)
            .build();

    // Test from/to bytes
    bytes = protoEntitySerDe.serialize(auditInfo1);
    auditInfoFromBytes =
        protoEntitySerDe.deserialize(
            bytes, com.datastrato.gravitino.meta.AuditInfo.class, Namespace.empty());
    Assertions.assertEquals(auditInfo1, auditInfoFromBytes);

    // Test with empty field
    com.datastrato.gravitino.meta.AuditInfo auditInfo2 =
        com.datastrato.gravitino.meta.AuditInfo.builder().build();

    byte[] bytes1 = protoEntitySerDe.serialize(auditInfo2);
    com.datastrato.gravitino.meta.AuditInfo auditInfoFromBytes1 =
        protoEntitySerDe.deserialize(
            bytes1, com.datastrato.gravitino.meta.AuditInfo.class, Namespace.empty());
    Assertions.assertEquals(auditInfo2, auditInfoFromBytes1);
  }

  @Test
  public void testEntitiesSerDe() throws IOException {
    Instant now = Instant.now();
    String creator = "creator";
    SchemaVersion version = SchemaVersion.V_0_1;
    Long metalakeId = 1L;
    String metalakeName = "metalake";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");

    com.datastrato.gravitino.meta.AuditInfo auditInfo =
        com.datastrato.gravitino.meta.AuditInfo.builder()
            .withCreator(creator)
            .withCreateTime(now)
            .build();

    // Test Metalake
    com.datastrato.gravitino.meta.BaseMetalake metalake =
        com.datastrato.gravitino.meta.BaseMetalake.builder()
            .withId(metalakeId)
            .withName(metalakeName)
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .withVersion(version)
            .build();

    ProtoEntitySerDe protoEntitySerDe = (ProtoEntitySerDe) entitySerDe;

    byte[] metalakeBytes = protoEntitySerDe.serialize(metalake);
    com.datastrato.gravitino.meta.BaseMetalake metalakeFromBytes =
        protoEntitySerDe.deserialize(
            metalakeBytes, com.datastrato.gravitino.meta.BaseMetalake.class, Namespace.empty());
    Assertions.assertEquals(metalake, metalakeFromBytes);

    // Test metalake without props map
    com.datastrato.gravitino.meta.BaseMetalake metalake1 =
        com.datastrato.gravitino.meta.BaseMetalake.builder()
            .withId(metalakeId)
            .withName(metalakeName)
            .withAuditInfo(auditInfo)
            .withVersion(version)
            .build();

    byte[] metalakeBytes1 = protoEntitySerDe.serialize(metalake1);
    com.datastrato.gravitino.meta.BaseMetalake metalakeFromBytes1 =
        protoEntitySerDe.deserialize(
            metalakeBytes1, com.datastrato.gravitino.meta.BaseMetalake.class, Namespace.empty());
    Assertions.assertEquals(metalake1, metalakeFromBytes1);

    // Test CatalogEntity
    Long catalogId = 1L;
    String catalogName = "catalog";
    String comment = "comment";
    String provider = "test";
    Namespace catalogNamespace = Namespace.of("metalake");

    com.datastrato.gravitino.meta.CatalogEntity catalogEntity =
        com.datastrato.gravitino.meta.CatalogEntity.builder()
            .withId(catalogId)
            .withName(catalogName)
            .withNamespace(catalogNamespace)
            .withComment(comment)
            .withType(com.datastrato.gravitino.Catalog.Type.RELATIONAL)
            .withProvider(provider)
            .withAuditInfo(auditInfo)
            .build();

    byte[] catalogBytes = protoEntitySerDe.serialize(catalogEntity);
    com.datastrato.gravitino.meta.CatalogEntity catalogEntityFromBytes =
        protoEntitySerDe.deserialize(
            catalogBytes, com.datastrato.gravitino.meta.CatalogEntity.class, catalogNamespace);
    Assertions.assertEquals(catalogEntity, catalogEntityFromBytes);

    // Test Fileset catalog
    com.datastrato.gravitino.meta.CatalogEntity filesetCatalogEntity =
        com.datastrato.gravitino.meta.CatalogEntity.builder()
            .withId(catalogId)
            .withName(catalogName)
            .withNamespace(catalogNamespace)
            .withComment(comment)
            .withType(com.datastrato.gravitino.Catalog.Type.FILESET)
            .withProvider(provider)
            .withAuditInfo(auditInfo)
            .build();
    byte[] filesetCatalogBytes = protoEntitySerDe.serialize(filesetCatalogEntity);
    com.datastrato.gravitino.meta.CatalogEntity filesetCatalogEntityFromBytes =
        protoEntitySerDe.deserialize(
            filesetCatalogBytes,
            com.datastrato.gravitino.meta.CatalogEntity.class,
            catalogNamespace);
    Assertions.assertEquals(filesetCatalogEntity, filesetCatalogEntityFromBytes);

    // Test SchemaEntity
    Namespace schemaNamespace = Namespace.of("metalake", "catalog");
    Long schemaId = 1L;
    String schemaName = "schema";
    com.datastrato.gravitino.meta.SchemaEntity schemaEntity =
        com.datastrato.gravitino.meta.SchemaEntity.builder()
            .withId(schemaId)
            .withName(schemaName)
            .withNamespace(schemaNamespace)
            .withAuditInfo(auditInfo)
            .build();

    byte[] schemaBytes = protoEntitySerDe.serialize(schemaEntity);
    com.datastrato.gravitino.meta.SchemaEntity schemaEntityFromBytes =
        protoEntitySerDe.deserialize(
            schemaBytes, com.datastrato.gravitino.meta.SchemaEntity.class, schemaNamespace);
    Assertions.assertEquals(schemaEntity, schemaEntityFromBytes);

    // Test SchemaEntity with additional fields
    com.datastrato.gravitino.meta.SchemaEntity schemaEntity1 =
        com.datastrato.gravitino.meta.SchemaEntity.builder()
            .withId(schemaId)
            .withName(schemaName)
            .withNamespace(schemaNamespace)
            .withAuditInfo(auditInfo)
            .withComment(comment)
            .withProperties(props)
            .build();
    byte[] schemaBytes1 = protoEntitySerDe.serialize(schemaEntity1);
    com.datastrato.gravitino.meta.SchemaEntity schemaEntityFromBytes1 =
        protoEntitySerDe.deserialize(
            schemaBytes1, com.datastrato.gravitino.meta.SchemaEntity.class, schemaNamespace);
    Assertions.assertEquals(schemaEntity1, schemaEntityFromBytes1);
    Assertions.assertEquals(comment, schemaEntityFromBytes1.comment());
    Assertions.assertEquals(props, schemaEntityFromBytes1.properties());

    // Test TableEntity
    Namespace tableNamespace = Namespace.of("metalake", "catalog", "schema");
    Long tableId = 1L;
    String tableName = "table";
    com.datastrato.gravitino.meta.TableEntity tableEntity =
        com.datastrato.gravitino.meta.TableEntity.builder()
            .withId(tableId)
            .withName(tableName)
            .withNamespace(tableNamespace)
            .withAuditInfo(auditInfo)
            .build();

    byte[] tableBytes = protoEntitySerDe.serialize(tableEntity);
    com.datastrato.gravitino.meta.TableEntity tableEntityFromBytes =
        protoEntitySerDe.deserialize(
            tableBytes, com.datastrato.gravitino.meta.TableEntity.class, tableNamespace);
    Assertions.assertEquals(tableEntity, tableEntityFromBytes);

    // Test FileEntity
    Namespace filesetNamespace = Namespace.of("metalake", "catalog", "schema");
    Long fileId = 1L;
    String fileName = "file";
    com.datastrato.gravitino.meta.FilesetEntity fileEntity =
        com.datastrato.gravitino.meta.FilesetEntity.builder()
            .withId(fileId)
            .withName(fileName)
            .withNamespace(filesetNamespace)
            .withAuditInfo(auditInfo)
            .withFilesetType(com.datastrato.gravitino.file.Fileset.Type.MANAGED)
            .withStorageLocation("testLocation")
            .withProperties(props)
            .withComment(comment)
            .build();
    byte[] fileBytes = protoEntitySerDe.serialize(fileEntity);
    com.datastrato.gravitino.meta.FilesetEntity fileEntityFromBytes =
        protoEntitySerDe.deserialize(
            fileBytes, com.datastrato.gravitino.meta.FilesetEntity.class, filesetNamespace);
    Assertions.assertEquals(fileEntity, fileEntityFromBytes);

    com.datastrato.gravitino.meta.FilesetEntity fileEntity1 =
        com.datastrato.gravitino.meta.FilesetEntity.builder()
            .withId(fileId)
            .withName(fileName)
            .withNamespace(filesetNamespace)
            .withAuditInfo(auditInfo)
            .withFilesetType(com.datastrato.gravitino.file.Fileset.Type.MANAGED)
            .withStorageLocation("testLocation")
            .build();
    byte[] fileBytes1 = protoEntitySerDe.serialize(fileEntity1);
    com.datastrato.gravitino.meta.FilesetEntity fileEntityFromBytes1 =
        protoEntitySerDe.deserialize(
            fileBytes1, com.datastrato.gravitino.meta.FilesetEntity.class, filesetNamespace);
    Assertions.assertEquals(fileEntity1, fileEntityFromBytes1);
    Assertions.assertNull(fileEntityFromBytes1.comment());
    Assertions.assertNull(fileEntityFromBytes1.properties());

    com.datastrato.gravitino.meta.FilesetEntity fileEntity2 =
        com.datastrato.gravitino.meta.FilesetEntity.builder()
            .withId(fileId)
            .withName(fileName)
            .withNamespace(filesetNamespace)
            .withAuditInfo(auditInfo)
            .withFilesetType(com.datastrato.gravitino.file.Fileset.Type.EXTERNAL)
            .withProperties(props)
            .withComment(comment)
            .withStorageLocation("testLocation")
            .build();
    byte[] fileBytes2 = protoEntitySerDe.serialize(fileEntity2);
    com.datastrato.gravitino.meta.FilesetEntity fileEntityFromBytes2 =
        protoEntitySerDe.deserialize(
            fileBytes2, com.datastrato.gravitino.meta.FilesetEntity.class, filesetNamespace);
    Assertions.assertEquals(fileEntity2, fileEntityFromBytes2);
    Assertions.assertEquals("testLocation", fileEntityFromBytes2.storageLocation());
    Assertions.assertEquals(
        com.datastrato.gravitino.file.Fileset.Type.EXTERNAL, fileEntityFromBytes2.filesetType());

    // Test TopicEntity
    Namespace topicNamespace = Namespace.of("metalake", "catalog", "default");
    Long topicId = 1L;
    String topicName = "topic";
    com.datastrato.gravitino.meta.TopicEntity topicEntity =
        com.datastrato.gravitino.meta.TopicEntity.builder()
            .withId(topicId)
            .withName(topicName)
            .withNamespace(topicNamespace)
            .withAuditInfo(auditInfo)
            .withComment(comment)
            .withProperties(props)
            .build();
    byte[] topicBytes = protoEntitySerDe.serialize(topicEntity);
    com.datastrato.gravitino.meta.TopicEntity topicEntityFromBytes =
        protoEntitySerDe.deserialize(
            topicBytes, com.datastrato.gravitino.meta.TopicEntity.class, topicNamespace);
    Assertions.assertEquals(topicEntity, topicEntityFromBytes);

    com.datastrato.gravitino.meta.TopicEntity topicEntity1 =
        com.datastrato.gravitino.meta.TopicEntity.builder()
            .withId(topicId)
            .withName(topicName)
            .withNamespace(topicNamespace)
            .withAuditInfo(auditInfo)
            .build();
    byte[] topicBytes1 = protoEntitySerDe.serialize(topicEntity1);
    com.datastrato.gravitino.meta.TopicEntity topicEntityFromBytes1 =
        protoEntitySerDe.deserialize(
            topicBytes1, com.datastrato.gravitino.meta.TopicEntity.class, topicNamespace);
    Assertions.assertEquals(topicEntity1, topicEntityFromBytes1);
    Assertions.assertNull(topicEntityFromBytes1.comment());
    Assertions.assertNull(topicEntityFromBytes1.properties());

    // Test UserEntity
    Namespace userNamespace =
        Namespace.of("metalake", Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.USER_SCHEMA_NAME);
    Long userId = 1L;
    String userName = "user";
    UserEntity userEntity =
        UserEntity.builder()
            .withId(userId)
            .withName(userName)
            .withNamespace(userNamespace)
            .withAuditInfo(auditInfo)
            .withRoleNames(Lists.newArrayList("role"))
            .withRoleIds(Lists.newArrayList(1L))
            .build();
    byte[] userBytes = protoEntitySerDe.serialize(userEntity);
    UserEntity userEntityFromBytes =
        protoEntitySerDe.deserialize(userBytes, UserEntity.class, userNamespace);
    Assertions.assertEquals(userEntity, userEntityFromBytes);

    UserEntity userEntityWithoutFields =
        UserEntity.builder()
            .withId(userId)
            .withName(userName)
            .withNamespace(userNamespace)
            .withAuditInfo(auditInfo)
            .build();
    userBytes = protoEntitySerDe.serialize(userEntityWithoutFields);
    userEntityFromBytes = protoEntitySerDe.deserialize(userBytes, UserEntity.class, userNamespace);
    Assertions.assertEquals(userEntityWithoutFields, userEntityFromBytes);
    Assertions.assertNull(userEntityWithoutFields.roles());
    Assertions.assertNull(userEntityWithoutFields.roleIds());

    // Test GroupEntity
    Namespace groupNamespace =
        Namespace.of("metalake", Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.GROUP_SCHEMA_NAME);
    Long groupId = 1L;
    String groupName = "group";

    GroupEntity group =
        GroupEntity.builder()
            .withId(groupId)
            .withName(groupName)
            .withNamespace(groupNamespace)
            .withAuditInfo(auditInfo)
            .withRoleNames(Lists.newArrayList("role"))
            .withRoleIds(Lists.newArrayList(1L))
            .build();
    byte[] groupBytes = protoEntitySerDe.serialize(group);
    GroupEntity groupFromBytes =
        protoEntitySerDe.deserialize(groupBytes, GroupEntity.class, groupNamespace);
    Assertions.assertEquals(group, groupFromBytes);

    GroupEntity groupWithoutFields =
        GroupEntity.builder()
            .withId(groupId)
            .withName(groupName)
            .withNamespace(groupNamespace)
            .withAuditInfo(auditInfo)
            .build();
    groupBytes = protoEntitySerDe.serialize(groupWithoutFields);
    groupFromBytes = protoEntitySerDe.deserialize(groupBytes, GroupEntity.class, groupNamespace);
    Assertions.assertEquals(groupWithoutFields, groupFromBytes);
    Assertions.assertNull(groupWithoutFields.roles());
    Assertions.assertNull(groupWithoutFields.roleIds());

    // Test RoleEntity
    Namespace roleNamespace =
        Namespace.of("metalake", Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.ROLE_SCHEMA_NAME);
    Long roleId = 1L;
    String roleName = "testRole";
    RoleEntity roleEntity =
        RoleEntity.builder()
            .withId(roleId)
            .withName(roleName)
            .withNamespace(roleNamespace)
            .withAuditInfo(auditInfo)
            .withSecurableObject(SecurableObjects.ofCatalog(catalogName))
            .withPrivileges(Lists.newArrayList(Privileges.UseCatalog.get()))
            .withProperties(props)
            .build();
    byte[] roleBytes = protoEntitySerDe.serialize(roleEntity);
    RoleEntity roleFromBytes =
        protoEntitySerDe.deserialize(roleBytes, RoleEntity.class, roleNamespace);
    Assertions.assertEquals(roleEntity, roleFromBytes);

    RoleEntity roleWithoutFields =
        RoleEntity.builder()
            .withId(1L)
            .withName(roleName)
            .withNamespace(roleNamespace)
            .withAuditInfo(auditInfo)
            .withSecurableObject(SecurableObjects.ofCatalog(catalogName))
            .withPrivileges(Lists.newArrayList(Privileges.UseCatalog.get()))
            .build();
    roleBytes = protoEntitySerDe.serialize(roleWithoutFields);
    roleFromBytes = protoEntitySerDe.deserialize(roleBytes, RoleEntity.class, roleNamespace);
    Assertions.assertEquals(roleWithoutFields, roleFromBytes);
  }
}
