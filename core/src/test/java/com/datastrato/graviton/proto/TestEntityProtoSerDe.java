/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.proto;

import com.datastrato.graviton.EntitySerDe;
import com.datastrato.graviton.EntitySerDeFactory;
import com.datastrato.graviton.meta.BaseMetalake;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.meta.SchemaVersion;
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

    com.datastrato.graviton.meta.AuditInfo auditInfo =
        new com.datastrato.graviton.meta.AuditInfo.Builder()
            .withCreator(creator)
            .withCreateTime(now)
            .withLastModifier(modifier)
            .withLastModifiedTime(now)
            .build();

    ProtoEntitySerDe protoEntitySerDe = (ProtoEntitySerDe) entitySerDe;

    AuditInfo auditInfoProto = protoEntitySerDe.toProto(auditInfo);
    Assertions.assertEquals(creator, auditInfoProto.getCreator());
    Assertions.assertEquals(now, ProtoUtils.toInstant(auditInfoProto.getCreateTime()));
    Assertions.assertEquals(modifier, auditInfoProto.getLastModifier());
    Assertions.assertEquals(now, ProtoUtils.toInstant(auditInfoProto.getLastModifiedTime()));

    com.datastrato.graviton.meta.AuditInfo auditInfoFromProto =
        protoEntitySerDe.fromProto(auditInfoProto);
    Assertions.assertEquals(auditInfo, auditInfoFromProto);

    // Test with optional fields
    com.datastrato.graviton.meta.AuditInfo auditInfo1 =
        new com.datastrato.graviton.meta.AuditInfo.Builder()
            .withCreator(creator)
            .withCreateTime(now)
            .build();

    AuditInfo auditInfoProto1 = protoEntitySerDe.toProto(auditInfo1);

    Assertions.assertEquals(creator, auditInfoProto1.getCreator());
    Assertions.assertEquals(now, ProtoUtils.toInstant(auditInfoProto1.getCreateTime()));

    com.datastrato.graviton.meta.AuditInfo auditInfoFromProto1 =
        protoEntitySerDe.fromProto(auditInfoProto1);
    Assertions.assertEquals(auditInfo1, auditInfoFromProto1);

    // Test from/to bytes
    byte[] bytes = entitySerDe.serialize(auditInfo1);
    com.datastrato.graviton.meta.AuditInfo auditInfoFromBytes =
        entitySerDe.deserialize(bytes, com.datastrato.graviton.meta.AuditInfo.class);
    Assertions.assertEquals(auditInfo1, auditInfoFromBytes);
  }

  @Test
  public void testEntitiesSerDe() throws IOException {
    Instant now = Instant.now();
    String creator = "creator";
    SchemaVersion version = SchemaVersion.V_0_1;
    Long metalakeId = 1L;
    String metalakeName = "metalake";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");

    com.datastrato.graviton.meta.AuditInfo auditInfo =
        new com.datastrato.graviton.meta.AuditInfo.Builder()
            .withCreator(creator)
            .withCreateTime(now)
            .build();

    // Test Metalake
    com.datastrato.graviton.meta.BaseMetalake metalake =
        new com.datastrato.graviton.meta.BaseMetalake.Builder()
            .withId(metalakeId)
            .withName(metalakeName)
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .withVersion(version)
            .build();

    ProtoEntitySerDe protoEntitySerDe = (ProtoEntitySerDe) entitySerDe;

    Metalake metalakeProto = protoEntitySerDe.toProto(metalake);
    Assertions.assertEquals(props, metalakeProto.getPropertiesMap());
    com.datastrato.graviton.meta.BaseMetalake metalakeFromProto =
        protoEntitySerDe.fromProto(metalakeProto);
    Assertions.assertEquals(metalake, metalakeFromProto);

    byte[] metalakeBytes = protoEntitySerDe.serialize(metalake);
    com.datastrato.graviton.meta.BaseMetalake metalakeFromBytes =
        protoEntitySerDe.deserialize(metalakeBytes, BaseMetalake.class);
    Assertions.assertEquals(metalake, metalakeFromBytes);

    // Test metalake without a property map
    com.datastrato.graviton.meta.BaseMetalake metalake1 =
        new com.datastrato.graviton.meta.BaseMetalake.Builder()
            .withId(metalakeId)
            .withName(metalakeName)
            .withAuditInfo(auditInfo)
            .withVersion(version)
            .build();

    Metalake metalakeProto1 = protoEntitySerDe.toProto(metalake1);
    Assertions.assertEquals(0, metalakeProto1.getPropertiesCount());
    com.datastrato.graviton.meta.BaseMetalake metalakeFromProto1 =
        protoEntitySerDe.fromProto(metalakeProto1);
    Assertions.assertEquals(metalake1, metalakeFromProto1);

    byte[] metalakeBytes1 = entitySerDe.serialize(metalake1);
    com.datastrato.graviton.meta.BaseMetalake metalakeFromBytes1 =
        entitySerDe.deserialize(metalakeBytes1, BaseMetalake.class);
    Assertions.assertEquals(metalake1, metalakeFromBytes1);

    // Test CatalogEntity
    Long catalogId = 1L;
    String catalogName = "catalog";
    String comment = "comment";

    com.datastrato.graviton.meta.CatalogEntity catalogEntity =
        new com.datastrato.graviton.meta.CatalogEntity.Builder()
            .withId(catalogId)
            .withMetalakeId(metalakeId)
            .withName(catalogName)
            .withComment(comment)
            .withType(com.datastrato.graviton.Catalog.Type.RELATIONAL)
            .withAuditInfo(auditInfo)
            .build();

    Catalog catalogProto = protoEntitySerDe.toProto(catalogEntity);
    com.datastrato.graviton.meta.CatalogEntity catalogEntityFromProto =
        protoEntitySerDe.fromProto(catalogProto);
    Assertions.assertEquals(catalogEntity, catalogEntityFromProto);

    byte[] catalogBytes = protoEntitySerDe.serialize(catalogEntity);
    com.datastrato.graviton.meta.CatalogEntity catalogEntityFromBytes =
        protoEntitySerDe.deserialize(catalogBytes, CatalogEntity.class);
    Assertions.assertEquals(catalogEntity, catalogEntityFromBytes);
  }
}
