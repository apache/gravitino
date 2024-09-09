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
package org.apache.gravitino.proto;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.EntitySerDe;
import org.apache.gravitino.EntitySerDeFactory;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.SchemaVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEntityProtoSerDe {

  private final EntitySerDe entitySerDe = EntitySerDeFactory.createEntitySerDe("proto");

  @Test
  public void testAuditInfoSerDe() throws IOException {
    Instant now = Instant.now();
    String creator = "creator";
    String modifier = "modifier";

    org.apache.gravitino.meta.AuditInfo auditInfo =
        org.apache.gravitino.meta.AuditInfo.builder()
            .withCreator(creator)
            .withCreateTime(now)
            .withLastModifier(modifier)
            .withLastModifiedTime(now)
            .build();

    ProtoEntitySerDe protoEntitySerDe = (ProtoEntitySerDe) entitySerDe;

    byte[] bytes = protoEntitySerDe.serialize(auditInfo);
    org.apache.gravitino.meta.AuditInfo auditInfoFromBytes =
        protoEntitySerDe.deserialize(
            bytes, org.apache.gravitino.meta.AuditInfo.class, Namespace.empty());
    Assertions.assertEquals(auditInfo, auditInfoFromBytes);

    // Test with optional fields
    org.apache.gravitino.meta.AuditInfo auditInfo1 =
        org.apache.gravitino.meta.AuditInfo.builder()
            .withCreator(creator)
            .withCreateTime(now)
            .build();

    // Test from/to bytes
    bytes = protoEntitySerDe.serialize(auditInfo1);
    auditInfoFromBytes =
        protoEntitySerDe.deserialize(
            bytes, org.apache.gravitino.meta.AuditInfo.class, Namespace.empty());
    Assertions.assertEquals(auditInfo1, auditInfoFromBytes);

    // Test with empty field
    org.apache.gravitino.meta.AuditInfo auditInfo2 =
        org.apache.gravitino.meta.AuditInfo.builder().build();

    byte[] bytes1 = protoEntitySerDe.serialize(auditInfo2);
    org.apache.gravitino.meta.AuditInfo auditInfoFromBytes1 =
        protoEntitySerDe.deserialize(
            bytes1, org.apache.gravitino.meta.AuditInfo.class, Namespace.empty());
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

    org.apache.gravitino.meta.AuditInfo auditInfo =
        org.apache.gravitino.meta.AuditInfo.builder()
            .withCreator(creator)
            .withCreateTime(now)
            .build();

    // Test Metalake
    org.apache.gravitino.meta.BaseMetalake metalake =
        org.apache.gravitino.meta.BaseMetalake.builder()
            .withId(metalakeId)
            .withName(metalakeName)
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .withVersion(version)
            .build();

    ProtoEntitySerDe protoEntitySerDe = (ProtoEntitySerDe) entitySerDe;

    byte[] metalakeBytes = protoEntitySerDe.serialize(metalake);
    org.apache.gravitino.meta.BaseMetalake metalakeFromBytes =
        protoEntitySerDe.deserialize(
            metalakeBytes, org.apache.gravitino.meta.BaseMetalake.class, Namespace.empty());
    Assertions.assertEquals(metalake, metalakeFromBytes);

    // Test metalake without props map
    org.apache.gravitino.meta.BaseMetalake metalake1 =
        org.apache.gravitino.meta.BaseMetalake.builder()
            .withId(metalakeId)
            .withName(metalakeName)
            .withAuditInfo(auditInfo)
            .withVersion(version)
            .build();

    byte[] metalakeBytes1 = protoEntitySerDe.serialize(metalake1);
    org.apache.gravitino.meta.BaseMetalake metalakeFromBytes1 =
        protoEntitySerDe.deserialize(
            metalakeBytes1, org.apache.gravitino.meta.BaseMetalake.class, Namespace.empty());
    Assertions.assertEquals(metalake1, metalakeFromBytes1);

    // Test CatalogEntity
    Long catalogId = 1L;
    String catalogName = "catalog";
    String comment = "comment";
    String provider = "test";
    Namespace catalogNamespace = Namespace.of("metalake");

    org.apache.gravitino.meta.CatalogEntity catalogEntity =
        org.apache.gravitino.meta.CatalogEntity.builder()
            .withId(catalogId)
            .withName(catalogName)
            .withNamespace(catalogNamespace)
            .withComment(comment)
            .withType(Catalog.Type.RELATIONAL)
            .withProvider(provider)
            .withAuditInfo(auditInfo)
            .build();

    byte[] catalogBytes = protoEntitySerDe.serialize(catalogEntity);
    org.apache.gravitino.meta.CatalogEntity catalogEntityFromBytes =
        protoEntitySerDe.deserialize(
            catalogBytes, org.apache.gravitino.meta.CatalogEntity.class, catalogNamespace);
    Assertions.assertEquals(catalogEntity, catalogEntityFromBytes);

    // Test Fileset catalog
    org.apache.gravitino.meta.CatalogEntity filesetCatalogEntity =
        org.apache.gravitino.meta.CatalogEntity.builder()
            .withId(catalogId)
            .withName(catalogName)
            .withNamespace(catalogNamespace)
            .withComment(comment)
            .withType(Catalog.Type.FILESET)
            .withProvider(provider)
            .withAuditInfo(auditInfo)
            .build();
    byte[] filesetCatalogBytes = protoEntitySerDe.serialize(filesetCatalogEntity);
    org.apache.gravitino.meta.CatalogEntity filesetCatalogEntityFromBytes =
        protoEntitySerDe.deserialize(
            filesetCatalogBytes, org.apache.gravitino.meta.CatalogEntity.class, catalogNamespace);
    Assertions.assertEquals(filesetCatalogEntity, filesetCatalogEntityFromBytes);

    // Test SchemaEntity
    Namespace schemaNamespace = Namespace.of("metalake", "catalog");
    Long schemaId = 1L;
    String schemaName = "schema";
    org.apache.gravitino.meta.SchemaEntity schemaEntity =
        org.apache.gravitino.meta.SchemaEntity.builder()
            .withId(schemaId)
            .withName(schemaName)
            .withNamespace(schemaNamespace)
            .withAuditInfo(auditInfo)
            .build();

    byte[] schemaBytes = protoEntitySerDe.serialize(schemaEntity);
    org.apache.gravitino.meta.SchemaEntity schemaEntityFromBytes =
        protoEntitySerDe.deserialize(
            schemaBytes, org.apache.gravitino.meta.SchemaEntity.class, schemaNamespace);
    Assertions.assertEquals(schemaEntity, schemaEntityFromBytes);

    // Test SchemaEntity with additional fields
    org.apache.gravitino.meta.SchemaEntity schemaEntity1 =
        org.apache.gravitino.meta.SchemaEntity.builder()
            .withId(schemaId)
            .withName(schemaName)
            .withNamespace(schemaNamespace)
            .withAuditInfo(auditInfo)
            .withComment(comment)
            .withProperties(props)
            .build();
    byte[] schemaBytes1 = protoEntitySerDe.serialize(schemaEntity1);
    org.apache.gravitino.meta.SchemaEntity schemaEntityFromBytes1 =
        protoEntitySerDe.deserialize(
            schemaBytes1, org.apache.gravitino.meta.SchemaEntity.class, schemaNamespace);
    Assertions.assertEquals(schemaEntity1, schemaEntityFromBytes1);
    Assertions.assertEquals(comment, schemaEntityFromBytes1.comment());
    Assertions.assertEquals(props, schemaEntityFromBytes1.properties());

    // Test TableEntity
    Namespace tableNamespace = Namespace.of("metalake", "catalog", "schema");
    Long tableId = 1L;
    String tableName = "table";
    org.apache.gravitino.meta.TableEntity tableEntity =
        org.apache.gravitino.meta.TableEntity.builder()
            .withId(tableId)
            .withName(tableName)
            .withNamespace(tableNamespace)
            .withAuditInfo(auditInfo)
            .build();

    byte[] tableBytes = protoEntitySerDe.serialize(tableEntity);
    org.apache.gravitino.meta.TableEntity tableEntityFromBytes =
        protoEntitySerDe.deserialize(
            tableBytes, org.apache.gravitino.meta.TableEntity.class, tableNamespace);
    Assertions.assertEquals(tableEntity, tableEntityFromBytes);

    // Test FileEntity
    Namespace filesetNamespace = Namespace.of("metalake", "catalog", "schema");
    Long fileId = 1L;
    String fileName = "file";
    org.apache.gravitino.meta.FilesetEntity fileEntity =
        org.apache.gravitino.meta.FilesetEntity.builder()
            .withId(fileId)
            .withName(fileName)
            .withNamespace(filesetNamespace)
            .withAuditInfo(auditInfo)
            .withFilesetType(Fileset.Type.MANAGED)
            .withStorageLocation("testLocation")
            .withProperties(props)
            .withComment(comment)
            .build();
    byte[] fileBytes = protoEntitySerDe.serialize(fileEntity);
    org.apache.gravitino.meta.FilesetEntity fileEntityFromBytes =
        protoEntitySerDe.deserialize(
            fileBytes, org.apache.gravitino.meta.FilesetEntity.class, filesetNamespace);
    Assertions.assertEquals(fileEntity, fileEntityFromBytes);

    org.apache.gravitino.meta.FilesetEntity fileEntity1 =
        org.apache.gravitino.meta.FilesetEntity.builder()
            .withId(fileId)
            .withName(fileName)
            .withNamespace(filesetNamespace)
            .withAuditInfo(auditInfo)
            .withFilesetType(Fileset.Type.MANAGED)
            .withStorageLocation("testLocation")
            .build();
    byte[] fileBytes1 = protoEntitySerDe.serialize(fileEntity1);
    org.apache.gravitino.meta.FilesetEntity fileEntityFromBytes1 =
        protoEntitySerDe.deserialize(
            fileBytes1, org.apache.gravitino.meta.FilesetEntity.class, filesetNamespace);
    Assertions.assertEquals(fileEntity1, fileEntityFromBytes1);
    Assertions.assertNull(fileEntityFromBytes1.comment());
    Assertions.assertNull(fileEntityFromBytes1.properties());

    org.apache.gravitino.meta.FilesetEntity fileEntity2 =
        org.apache.gravitino.meta.FilesetEntity.builder()
            .withId(fileId)
            .withName(fileName)
            .withNamespace(filesetNamespace)
            .withAuditInfo(auditInfo)
            .withFilesetType(Fileset.Type.EXTERNAL)
            .withProperties(props)
            .withComment(comment)
            .withStorageLocation("testLocation")
            .build();
    byte[] fileBytes2 = protoEntitySerDe.serialize(fileEntity2);
    org.apache.gravitino.meta.FilesetEntity fileEntityFromBytes2 =
        protoEntitySerDe.deserialize(
            fileBytes2, org.apache.gravitino.meta.FilesetEntity.class, filesetNamespace);
    Assertions.assertEquals(fileEntity2, fileEntityFromBytes2);
    Assertions.assertEquals("testLocation", fileEntityFromBytes2.storageLocation());
    Assertions.assertEquals(Fileset.Type.EXTERNAL, fileEntityFromBytes2.filesetType());

    // Test TopicEntity
    Namespace topicNamespace = Namespace.of("metalake", "catalog", "default");
    Long topicId = 1L;
    String topicName = "topic";
    org.apache.gravitino.meta.TopicEntity topicEntity =
        org.apache.gravitino.meta.TopicEntity.builder()
            .withId(topicId)
            .withName(topicName)
            .withNamespace(topicNamespace)
            .withAuditInfo(auditInfo)
            .withComment(comment)
            .withProperties(props)
            .build();
    byte[] topicBytes = protoEntitySerDe.serialize(topicEntity);
    org.apache.gravitino.meta.TopicEntity topicEntityFromBytes =
        protoEntitySerDe.deserialize(
            topicBytes, org.apache.gravitino.meta.TopicEntity.class, topicNamespace);
    Assertions.assertEquals(topicEntity, topicEntityFromBytes);

    org.apache.gravitino.meta.TopicEntity topicEntity1 =
        org.apache.gravitino.meta.TopicEntity.builder()
            .withId(topicId)
            .withName(topicName)
            .withNamespace(topicNamespace)
            .withAuditInfo(auditInfo)
            .build();
    byte[] topicBytes1 = protoEntitySerDe.serialize(topicEntity1);
    org.apache.gravitino.meta.TopicEntity topicEntityFromBytes1 =
        protoEntitySerDe.deserialize(
            topicBytes1, org.apache.gravitino.meta.TopicEntity.class, topicNamespace);
    Assertions.assertEquals(topicEntity1, topicEntityFromBytes1);
    Assertions.assertNull(topicEntityFromBytes1.comment());
    Assertions.assertNull(topicEntityFromBytes1.properties());
  }
}
