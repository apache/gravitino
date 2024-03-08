/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.EntitySerDe;
import com.datastrato.gravitino.EntitySerDeFactory;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.google.common.collect.ImmutableMap;
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
        protoEntitySerDe.deserialize(bytes, com.datastrato.gravitino.meta.AuditInfo.class);
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
        protoEntitySerDe.deserialize(bytes, com.datastrato.gravitino.meta.AuditInfo.class);
    Assertions.assertEquals(auditInfo1, auditInfoFromBytes);

    // Test with empty field
    com.datastrato.gravitino.meta.AuditInfo auditInfo2 =
        com.datastrato.gravitino.meta.AuditInfo.builder().build();

    byte[] bytes1 = protoEntitySerDe.serialize(auditInfo2);
    com.datastrato.gravitino.meta.AuditInfo auditInfoFromBytes1 =
        protoEntitySerDe.deserialize(bytes1, com.datastrato.gravitino.meta.AuditInfo.class);
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
        new com.datastrato.gravitino.meta.BaseMetalake.Builder()
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
            metalakeBytes, com.datastrato.gravitino.meta.BaseMetalake.class);
    Assertions.assertEquals(metalake, metalakeFromBytes);

    // Test metalake without props map
    com.datastrato.gravitino.meta.BaseMetalake metalake1 =
        new com.datastrato.gravitino.meta.BaseMetalake.Builder()
            .withId(metalakeId)
            .withName(metalakeName)
            .withAuditInfo(auditInfo)
            .withVersion(version)
            .build();

    byte[] metalakeBytes1 = protoEntitySerDe.serialize(metalake1);
    com.datastrato.gravitino.meta.BaseMetalake metalakeFromBytes1 =
        protoEntitySerDe.deserialize(
            metalakeBytes1, com.datastrato.gravitino.meta.BaseMetalake.class);
    Assertions.assertEquals(metalake1, metalakeFromBytes1);

    // Test CatalogEntity
    Long catalogId = 1L;
    String catalogName = "catalog";
    String comment = "comment";
    String provider = "test";

    com.datastrato.gravitino.meta.CatalogEntity catalogEntity =
        com.datastrato.gravitino.meta.CatalogEntity.builder()
            .withId(catalogId)
            .withName(catalogName)
            .withComment(comment)
            .withType(com.datastrato.gravitino.Catalog.Type.RELATIONAL)
            .withProvider(provider)
            .withAuditInfo(auditInfo)
            .build();

    byte[] catalogBytes = protoEntitySerDe.serialize(catalogEntity);
    com.datastrato.gravitino.meta.CatalogEntity catalogEntityFromBytes =
        protoEntitySerDe.deserialize(
            catalogBytes, com.datastrato.gravitino.meta.CatalogEntity.class);
    Assertions.assertEquals(catalogEntity, catalogEntityFromBytes);

    // Test Fileset catalog
    com.datastrato.gravitino.meta.CatalogEntity filesetCatalogEntity =
        com.datastrato.gravitino.meta.CatalogEntity.builder()
            .withId(catalogId)
            .withName(catalogName)
            .withComment(comment)
            .withType(com.datastrato.gravitino.Catalog.Type.FILESET)
            .withProvider(provider)
            .withAuditInfo(auditInfo)
            .build();
    byte[] filesetCatalogBytes = protoEntitySerDe.serialize(filesetCatalogEntity);
    com.datastrato.gravitino.meta.CatalogEntity filesetCatalogEntityFromBytes =
        protoEntitySerDe.deserialize(
            filesetCatalogBytes, com.datastrato.gravitino.meta.CatalogEntity.class);
    Assertions.assertEquals(filesetCatalogEntity, filesetCatalogEntityFromBytes);

    // Test SchemaEntity
    Long schemaId = 1L;
    String schemaName = "schema";
    com.datastrato.gravitino.meta.SchemaEntity schemaEntity =
        new com.datastrato.gravitino.meta.SchemaEntity.Builder()
            .withId(schemaId)
            .withName(schemaName)
            .withAuditInfo(auditInfo)
            .build();

    byte[] schemaBytes = protoEntitySerDe.serialize(schemaEntity);
    com.datastrato.gravitino.meta.SchemaEntity schemaEntityFromBytes =
        protoEntitySerDe.deserialize(schemaBytes, com.datastrato.gravitino.meta.SchemaEntity.class);
    Assertions.assertEquals(schemaEntity, schemaEntityFromBytes);

    // Test SchemaEntity with additional fields
    com.datastrato.gravitino.meta.SchemaEntity schemaEntity1 =
        new com.datastrato.gravitino.meta.SchemaEntity.Builder()
            .withId(schemaId)
            .withName(schemaName)
            .withAuditInfo(auditInfo)
            .withComment(comment)
            .withProperties(props)
            .build();
    byte[] schemaBytes1 = protoEntitySerDe.serialize(schemaEntity1);
    com.datastrato.gravitino.meta.SchemaEntity schemaEntityFromBytes1 =
        protoEntitySerDe.deserialize(
            schemaBytes1, com.datastrato.gravitino.meta.SchemaEntity.class);
    Assertions.assertEquals(schemaEntity1, schemaEntityFromBytes1);
    Assertions.assertEquals(comment, schemaEntityFromBytes1.comment());
    Assertions.assertEquals(props, schemaEntityFromBytes1.properties());

    // Test TableEntity
    Long tableId = 1L;
    String tableName = "table";
    com.datastrato.gravitino.meta.TableEntity tableEntity =
        new com.datastrato.gravitino.meta.TableEntity.Builder()
            .withId(tableId)
            .withName(tableName)
            .withAuditInfo(auditInfo)
            .build();

    byte[] tableBytes = protoEntitySerDe.serialize(tableEntity);
    com.datastrato.gravitino.meta.TableEntity tableEntityFromBytes =
        protoEntitySerDe.deserialize(tableBytes, com.datastrato.gravitino.meta.TableEntity.class);
    Assertions.assertEquals(tableEntity, tableEntityFromBytes);

    // Test FileEntity
    Long fileId = 1L;
    String fileName = "file";
    com.datastrato.gravitino.meta.FilesetEntity fileEntity =
        new com.datastrato.gravitino.meta.FilesetEntity.Builder()
            .withId(fileId)
            .withName(fileName)
            .withAuditInfo(auditInfo)
            .withFilesetType(com.datastrato.gravitino.file.Fileset.Type.MANAGED)
            .withStorageLocation("testLocation")
            .withProperties(props)
            .withComment(comment)
            .build();
    byte[] fileBytes = protoEntitySerDe.serialize(fileEntity);
    com.datastrato.gravitino.meta.FilesetEntity fileEntityFromBytes =
        protoEntitySerDe.deserialize(fileBytes, com.datastrato.gravitino.meta.FilesetEntity.class);
    Assertions.assertEquals(fileEntity, fileEntityFromBytes);

    com.datastrato.gravitino.meta.FilesetEntity fileEntity1 =
        new com.datastrato.gravitino.meta.FilesetEntity.Builder()
            .withId(fileId)
            .withName(fileName)
            .withAuditInfo(auditInfo)
            .withFilesetType(com.datastrato.gravitino.file.Fileset.Type.MANAGED)
            .withStorageLocation("testLocation")
            .build();
    byte[] fileBytes1 = protoEntitySerDe.serialize(fileEntity1);
    com.datastrato.gravitino.meta.FilesetEntity fileEntityFromBytes1 =
        protoEntitySerDe.deserialize(fileBytes1, com.datastrato.gravitino.meta.FilesetEntity.class);
    Assertions.assertEquals(fileEntity1, fileEntityFromBytes1);
    Assertions.assertNull(fileEntityFromBytes1.comment());
    Assertions.assertNull(fileEntityFromBytes1.properties());

    com.datastrato.gravitino.meta.FilesetEntity fileEntity2 =
        new com.datastrato.gravitino.meta.FilesetEntity.Builder()
            .withId(fileId)
            .withName(fileName)
            .withAuditInfo(auditInfo)
            .withFilesetType(com.datastrato.gravitino.file.Fileset.Type.EXTERNAL)
            .withProperties(props)
            .withComment(comment)
            .withStorageLocation("testLocation")
            .build();
    byte[] fileBytes2 = protoEntitySerDe.serialize(fileEntity2);
    com.datastrato.gravitino.meta.FilesetEntity fileEntityFromBytes2 =
        protoEntitySerDe.deserialize(fileBytes2, com.datastrato.gravitino.meta.FilesetEntity.class);
    Assertions.assertEquals(fileEntity2, fileEntityFromBytes2);
    Assertions.assertEquals("testLocation", fileEntityFromBytes2.storageLocation());
    Assertions.assertEquals(
        com.datastrato.gravitino.file.Fileset.Type.EXTERNAL, fileEntityFromBytes2.filesetType());

    // Test TopicEntity
    Long topicId = 1L;
    String topicName = "topic";
    com.datastrato.gravitino.meta.TopicEntity topicEntity =
        com.datastrato.gravitino.meta.TopicEntity.builder()
            .withId(topicId)
            .withName(topicName)
            .withAuditInfo(auditInfo)
            .withComment(comment)
            .withProperties(props)
            .build();
    byte[] topicBytes = protoEntitySerDe.serialize(topicEntity);
    com.datastrato.gravitino.meta.TopicEntity topicEntityFromBytes =
        protoEntitySerDe.deserialize(topicBytes, com.datastrato.gravitino.meta.TopicEntity.class);
    Assertions.assertEquals(topicEntity, topicEntityFromBytes);

    com.datastrato.gravitino.meta.TopicEntity topicEntity1 =
        com.datastrato.gravitino.meta.TopicEntity.builder()
            .withId(topicId)
            .withName(topicName)
            .withAuditInfo(auditInfo)
            .build();
    byte[] topicBytes1 = protoEntitySerDe.serialize(topicEntity1);
    com.datastrato.gravitino.meta.TopicEntity topicEntityFromBytes1 =
        protoEntitySerDe.deserialize(topicBytes1, com.datastrato.gravitino.meta.TopicEntity.class);
    Assertions.assertEquals(topicEntity1, topicEntityFromBytes1);
    Assertions.assertNull(topicEntityFromBytes1.comment());
    Assertions.assertNull(topicEntityFromBytes1.properties());
  }
}
