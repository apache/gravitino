package com.datastrato.graviton.proto;

import com.datastrato.graviton.meta.SchemaVersion;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEntityProtoSerDe {

  @Test
  public void testAuditInfoSerDe() {
    Instant now = Instant.now();
    String creator = "creator";
    String modifier = "modifier";
    String accessor = "accessor";

    com.datastrato.graviton.meta.AuditInfo auditInfo =
        new com.datastrato.graviton.meta.AuditInfo.Builder()
            .withCreator(creator)
            .withCreateTime(now)
            .withLastModifier(modifier)
            .withLastModifiedTime(now)
            .build();

    AuditInfo auditInfoProto = ProtoUtils.toProto(auditInfo);
    Assertions.assertEquals(creator, auditInfoProto.getCreator());
    Assertions.assertEquals(now, ProtoUtils.toInstant(auditInfoProto.getCreateTime()));
    Assertions.assertEquals(modifier, auditInfoProto.getLastModifier());
    Assertions.assertEquals(now, ProtoUtils.toInstant(auditInfoProto.getLastModifiedTime()));

    com.datastrato.graviton.meta.AuditInfo auditInfoFromProto =
        ProtoUtils.fromProto(auditInfoProto);
    Assertions.assertEquals(auditInfo, auditInfoFromProto);

    // Test with optional fields
    com.datastrato.graviton.meta.AuditInfo auditInfo1 =
        new com.datastrato.graviton.meta.AuditInfo.Builder()
            .withCreator(creator)
            .withCreateTime(now)
            .build();

    AuditInfo auditInfoProto1 = ProtoUtils.toProto(auditInfo1);

    Assertions.assertEquals(creator, auditInfoProto1.getCreator());
    Assertions.assertEquals(now, ProtoUtils.toInstant(auditInfoProto1.getCreateTime()));

    com.datastrato.graviton.meta.AuditInfo auditInfoFromProto1 =
        ProtoUtils.fromProto(auditInfoProto1);
    Assertions.assertEquals(auditInfo1, auditInfoFromProto1);

    // Test from/to bytes
    byte[] bytes = ProtoUtils.toBytes(auditInfo1);
    com.datastrato.graviton.meta.AuditInfo auditInfoFromBytes =
        ProtoUtils.fromBytes(bytes, com.datastrato.graviton.meta.AuditInfo.class);
    Assertions.assertEquals(auditInfo1, auditInfoFromBytes);
  }

  @Test
  public void testEntitiesSerDe() {
    Instant now = Instant.now();
    String creator = "creator";
    Integer tenantId = 1;
    SchemaVersion version = SchemaVersion.V_0_1;
    Long lakehouseId = 1L;
    String lakehouseName = "lakehouse";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");

    com.datastrato.graviton.meta.AuditInfo auditInfo =
        new com.datastrato.graviton.meta.AuditInfo.Builder()
            .withCreator(creator)
            .withCreateTime(now)
            .build();

    // Test Lakehouse
    com.datastrato.graviton.meta.Lakehouse lakehouse =
        new com.datastrato.graviton.meta.Lakehouse.Builder()
            .withId(lakehouseId)
            .withName(lakehouseName)
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .withVersion(version)
            .build();

    Lakehouse lakehouseProto = ProtoUtils.toProto(lakehouse);
    Assertions.assertEquals(props, lakehouseProto.getPropertiesMap());
    com.datastrato.graviton.meta.Lakehouse lakehouseFromProto =
        ProtoUtils.fromProto(lakehouseProto);
    Assertions.assertEquals(lakehouse, lakehouseFromProto);

    byte[] lakehouseBytes = ProtoUtils.toBytes(lakehouse);
    com.datastrato.graviton.meta.Lakehouse lakehouseFromBytes =
        ProtoUtils.fromBytes(lakehouseBytes, com.datastrato.graviton.meta.Lakehouse.class);
    Assertions.assertEquals(lakehouse, lakehouseFromBytes);

    // Test Lakehouse without props map
    com.datastrato.graviton.meta.Lakehouse lakehouse1 =
        new com.datastrato.graviton.meta.Lakehouse.Builder()
            .withId(lakehouseId)
            .withName(lakehouseName)
            .withAuditInfo(auditInfo)
            .withVersion(version)
            .build();

    Lakehouse lakehouseProto1 = ProtoUtils.toProto(lakehouse1);
    Assertions.assertEquals(0, lakehouseProto1.getPropertiesCount());
    com.datastrato.graviton.meta.Lakehouse lakehouseFromProto1 =
        ProtoUtils.fromProto(lakehouseProto1);
    Assertions.assertEquals(lakehouse1, lakehouseFromProto1);

    byte[] lakehouseBytes1 = ProtoUtils.toBytes(lakehouse1);
    com.datastrato.graviton.meta.Lakehouse lakehouseFromBytes1 =
        ProtoUtils.fromBytes(lakehouseBytes1, com.datastrato.graviton.meta.Lakehouse.class);
    Assertions.assertEquals(lakehouse1, lakehouseFromBytes1);
  }
}
