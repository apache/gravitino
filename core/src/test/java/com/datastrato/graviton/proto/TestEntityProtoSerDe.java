package com.datastrato.graviton.proto;

import com.datastrato.graviton.EntitySerDe;
import com.datastrato.graviton.EntitySerDeFactory;
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

    ProtoEntitySerDe protoEntitySerDe = (ProtoEntitySerDe) entitySerDe;

    Lakehouse lakehouseProto = protoEntitySerDe.toProto(lakehouse);
    Assertions.assertEquals(props, lakehouseProto.getPropertiesMap());
    com.datastrato.graviton.meta.Lakehouse lakehouseFromProto =
        protoEntitySerDe.fromProto(lakehouseProto);
    Assertions.assertEquals(lakehouse, lakehouseFromProto);

    byte[] lakehouseBytes = protoEntitySerDe.serialize(lakehouse);
    com.datastrato.graviton.meta.Lakehouse lakehouseFromBytes =
        protoEntitySerDe.deserialize(lakehouseBytes, com.datastrato.graviton.meta.Lakehouse.class);
    Assertions.assertEquals(lakehouse, lakehouseFromBytes);

    // Test Lakehouse without props map
    com.datastrato.graviton.meta.Lakehouse lakehouse1 =
        new com.datastrato.graviton.meta.Lakehouse.Builder()
            .withId(lakehouseId)
            .withName(lakehouseName)
            .withAuditInfo(auditInfo)
            .withVersion(version)
            .build();

    Lakehouse lakehouseProto1 = protoEntitySerDe.toProto(lakehouse1);
    Assertions.assertEquals(0, lakehouseProto1.getPropertiesCount());
    com.datastrato.graviton.meta.Lakehouse lakehouseFromProto1 =
        protoEntitySerDe.fromProto(lakehouseProto1);
    Assertions.assertEquals(lakehouse1, lakehouseFromProto1);

    byte[] lakehouseBytes1 = entitySerDe.serialize(lakehouse1);
    com.datastrato.graviton.meta.Lakehouse lakehouseFromBytes1 =
        entitySerDe.deserialize(lakehouseBytes1, com.datastrato.graviton.meta.Lakehouse.class);
    Assertions.assertEquals(lakehouse1, lakehouseFromBytes1);
  }
}
