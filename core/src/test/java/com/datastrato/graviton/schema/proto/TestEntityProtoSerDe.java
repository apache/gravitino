package com.datastrato.graviton.schema.proto;

import com.datastrato.graviton.schema.HasExtraInfo;
import com.datastrato.graviton.schema.SchemaVersion;
import com.datastrato.graviton.schema.VirtualTableInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
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

    com.datastrato.graviton.schema.AuditInfo auditInfo =
        new com.datastrato.graviton.schema.AuditInfo.Builder()
            .withCreator(creator)
            .withCreateTime(now)
            .withLastModifier(modifier)
            .withLastModifiedTime(now)
            .withLastAccessUser(accessor)
            .withLastAccessTime(now)
            .build();

    AuditInfo auditInfoProto = ProtoUtils.toProto(auditInfo);
    Assertions.assertEquals(creator, auditInfoProto.getCreator());
    Assertions.assertEquals(now, ProtoUtils.toInstant(auditInfoProto.getCreateTime()));
    Assertions.assertEquals(modifier, auditInfoProto.getLastModifier());
    Assertions.assertEquals(now, ProtoUtils.toInstant(auditInfoProto.getLastModifiedTime()));
    Assertions.assertEquals(accessor, auditInfoProto.getLastAccessUser());
    Assertions.assertEquals(now, ProtoUtils.toInstant(auditInfoProto.getLastAccessTime()));

    com.datastrato.graviton.schema.AuditInfo auditInfoFromProto =
        ProtoUtils.fromProto(auditInfoProto);
    Assertions.assertEquals(auditInfo, auditInfoFromProto);

    // Test with optional fields
    com.datastrato.graviton.schema.AuditInfo auditInfo1 =
        new com.datastrato.graviton.schema.AuditInfo.Builder()
            .withCreator(creator)
            .withCreateTime(now)
            .build();

    AuditInfo auditInfoProto1 = ProtoUtils.toProto(auditInfo1);

    Assertions.assertEquals(creator, auditInfoProto1.getCreator());
    Assertions.assertEquals(now, ProtoUtils.toInstant(auditInfoProto1.getCreateTime()));

    com.datastrato.graviton.schema.AuditInfo auditInfoFromProto1 =
        ProtoUtils.fromProto(auditInfoProto1);
    Assertions.assertEquals(auditInfo1, auditInfoFromProto1);

    // Test from/to bytes
    byte[] bytes = ProtoUtils.toBytes(auditInfo1);
    com.datastrato.graviton.schema.AuditInfo auditInfoFromBytes =
        ProtoUtils.fromBytes(
            bytes, com.datastrato.graviton.schema.AuditInfo.class, AuditInfo.parser());
    Assertions.assertEquals(auditInfo1, auditInfoFromBytes);
  }

  @Test
  public void testEntitiesSerDe() {
    Instant now = Instant.now();
    String creator = "creator";
    Integer tenantId = 1;
    String tenantName = "tenant";
    SchemaVersion version = SchemaVersion.V_0_1;
    Long lakehouseId = 1L;
    String lakehouseName = "lakehouse";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Long zoneId = 1L;
    String zoneName = "zone";
    Type columnType = TypeCreator.NULLABLE.STRING;

    com.datastrato.graviton.schema.AuditInfo auditInfo =
        new com.datastrato.graviton.schema.AuditInfo.Builder()
            .withCreator(creator)
            .withCreateTime(now)
            .build();

    // Test Tenant
    com.datastrato.graviton.schema.Tenant tenant =
        new com.datastrato.graviton.schema.Tenant.Builder()
            .withId(tenantId)
            .withName(tenantName)
            .withVersion(version)
            .withAuditInfo(auditInfo)
            .build();

    Tenant tenantProto = ProtoUtils.toProto(tenant);
    com.datastrato.graviton.schema.Tenant tenantFromProto = ProtoUtils.fromProto(tenantProto);
    Assertions.assertEquals(tenant, tenantFromProto);

    byte[] tenantBytes = ProtoUtils.toBytes(tenant);
    com.datastrato.graviton.schema.Tenant tenantFromBytes =
        ProtoUtils.fromBytes(
            tenantBytes, com.datastrato.graviton.schema.Tenant.class, Tenant.parser());
    Assertions.assertEquals(tenant, tenantFromBytes);

    // Test Lakehouse
    com.datastrato.graviton.schema.Lakehouse lakehouse =
        new com.datastrato.graviton.schema.Lakehouse.Builder()
            .withId(lakehouseId)
            .withName(lakehouseName)
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();

    Lakehouse lakehouseProto = ProtoUtils.toProto(lakehouse);
    Assertions.assertEquals(props, lakehouseProto.getPropertiesMap());
    com.datastrato.graviton.schema.Lakehouse lakehouseFromProto =
        ProtoUtils.fromProto(lakehouseProto);
    Assertions.assertEquals(lakehouse, lakehouseFromProto);

    byte[] lakehouseBytes = ProtoUtils.toBytes(lakehouse);
    com.datastrato.graviton.schema.Lakehouse lakehouseFromBytes =
        ProtoUtils.fromBytes(
            lakehouseBytes, com.datastrato.graviton.schema.Lakehouse.class, Lakehouse.parser());
    Assertions.assertEquals(lakehouse, lakehouseFromBytes);

    // Test Lakehouse without props map
    com.datastrato.graviton.schema.Lakehouse lakehouse1 =
        new com.datastrato.graviton.schema.Lakehouse.Builder()
            .withId(lakehouseId)
            .withName(lakehouseName)
            .withAuditInfo(auditInfo)
            .build();

    Lakehouse lakehouseProto1 = ProtoUtils.toProto(lakehouse1);
    Assertions.assertEquals(0, lakehouseProto1.getPropertiesCount());
    com.datastrato.graviton.schema.Lakehouse lakehouseFromProto1 =
        ProtoUtils.fromProto(lakehouseProto1);
    Assertions.assertEquals(lakehouse1, lakehouseFromProto1);

    byte[] lakehouseBytes1 = ProtoUtils.toBytes(lakehouse1);
    com.datastrato.graviton.schema.Lakehouse lakehouseFromBytes1 =
        ProtoUtils.fromBytes(
            lakehouseBytes1, com.datastrato.graviton.schema.Lakehouse.class, Lakehouse.parser());
    Assertions.assertEquals(lakehouse1, lakehouseFromBytes1);

    // Test Zone
    com.datastrato.graviton.schema.Zone zone =
        new com.datastrato.graviton.schema.Zone.Builder()
            .withId(zoneId)
            .withLakehouseId(lakehouseId)
            .withName(zoneName)
            .withAuditInfo(auditInfo)
            .build();

    Zone zoneProto = ProtoUtils.toProto(zone);
    com.datastrato.graviton.schema.Zone zoneFromProto = ProtoUtils.fromProto(zoneProto);
    Assertions.assertEquals(zone, zoneFromProto);

    byte[] zoneBytes = ProtoUtils.toBytes(zone);
    com.datastrato.graviton.schema.Zone zoneFromBytes =
        ProtoUtils.fromBytes(zoneBytes, com.datastrato.graviton.schema.Zone.class, Zone.parser());
    Assertions.assertEquals(zone, zoneFromBytes);

    // Test Column
    com.datastrato.graviton.schema.Column column =
        new com.datastrato.graviton.schema.Column.Builder()
            .withId(1)
            .withEntityId(1L)
            .withEntitySnapshotId(1L)
            .withName("col1")
            .withType(columnType)
            .withPosition(1)
            .withAuditInfo(auditInfo)
            .build();

    Column columnProto = ProtoUtils.toProto(column);
    com.datastrato.graviton.schema.Column columnFromProto = ProtoUtils.fromProto(columnProto);
    Assertions.assertEquals(column, columnFromProto);
    Assertions.assertEquals(columnType, columnFromProto.getType());

    byte[] columnBytes = ProtoUtils.toBytes(column);
    com.datastrato.graviton.schema.Column columnFromBytes =
        ProtoUtils.fromBytes(
            columnBytes, com.datastrato.graviton.schema.Column.class, Column.parser());
    Assertions.assertEquals(column, columnFromBytes);
  }

  @Test
  public void testTableSerDe() {
    com.datastrato.graviton.schema.Table.TableType tableType =
        com.datastrato.graviton.schema.Table.TableType.VIRTUAL;
    HasExtraInfo.ExtraInfo extraInfo = new VirtualTableInfo(1, ImmutableList.of("db", "table"));
    com.datastrato.graviton.schema.AuditInfo auditInfo =
        new com.datastrato.graviton.schema.AuditInfo.Builder()
            .withCreator("creator")
            .withCreateTime(Instant.now())
            .build();

    com.datastrato.graviton.schema.Table table =
        new com.datastrato.graviton.schema.Table.Builder()
            .withId(1L)
            .withZoneId(1L)
            .withName("table")
            .withType(tableType)
            .withSnapshotId(1L)
            .withAuditInfo(auditInfo)
            .withExtraInfo(extraInfo)
            .build();

    Table tableProto = ProtoUtils.toProto(table);
    Assertions.assertEquals(tableType.getTypeStr(), tableProto.getType().name());

    com.datastrato.graviton.schema.Table tableFromProto = ProtoUtils.fromProto(tableProto);
    Assertions.assertEquals(extraInfo, tableFromProto.getExtraInfo());
    Assertions.assertEquals(table, tableFromProto);

    byte[] tableBytes = ProtoUtils.toBytes(table);
    com.datastrato.graviton.schema.Table tableFromBytes =
        ProtoUtils.fromBytes(
            tableBytes, com.datastrato.graviton.schema.Table.class, Table.parser());
    Assertions.assertEquals(table, tableFromBytes);

    // Test Table with unsupported type
    com.datastrato.graviton.schema.Table table1 =
        new com.datastrato.graviton.schema.Table.Builder()
            .withId(1L)
            .withZoneId(1L)
            .withName("table")
            .withType(com.datastrato.graviton.schema.Table.TableType.VIEW)
            .withSnapshotId(1L)
            .withAuditInfo(auditInfo)
            .withExtraInfo(extraInfo)
            .build();

    Throwable exception =
        Assertions.assertThrows(
            ProtoSerDeException.class,
            () -> {
              ProtoUtils.toProto(table1);
            });
    Assertions.assertEquals("Table type VIEW is not supported yet.", exception.getMessage());
  }
}
