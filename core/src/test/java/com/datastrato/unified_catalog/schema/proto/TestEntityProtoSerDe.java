package com.datastrato.unified_catalog.schema.proto;

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

    com.datastrato.unified_catalog.schema.AuditInfo auditInfo =
        new com.datastrato.unified_catalog.schema.AuditInfo.Builder()
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

    com.datastrato.unified_catalog.schema.AuditInfo auditInfoFromProto =
        ProtoUtils.fromProto(auditInfoProto);
    Assertions.assertEquals(auditInfo, auditInfoFromProto);

    // Test with optional fields
    com.datastrato.unified_catalog.schema.AuditInfo auditInfo1 =
        new com.datastrato.unified_catalog.schema.AuditInfo.Builder()
            .withCreator(creator)
            .withCreateTime(now)
            .build();

    AuditInfo auditInfoProto1 = ProtoUtils.toProto(auditInfo1);

    Assertions.assertEquals(creator, auditInfoProto1.getCreator());
    Assertions.assertEquals(now, ProtoUtils.toInstant(auditInfoProto1.getCreateTime()));

    com.datastrato.unified_catalog.schema.AuditInfo auditInfoFromProto1 =
        ProtoUtils.fromProto(auditInfoProto1);
    Assertions.assertEquals(auditInfo1, auditInfoFromProto1);

    // Test from/to bytes
    byte[] bytes = ProtoUtils.toBytes(auditInfo1);
    com.datastrato.unified_catalog.schema.AuditInfo auditInfoFromBytes =
        ProtoUtils.fromBytes(
            bytes, com.datastrato.unified_catalog.schema.AuditInfo.class, AuditInfo.parser());
    Assertions.assertEquals(auditInfo1, auditInfoFromBytes);
  }

  @Test
  public void testEntitiesSerDe() {
    Instant now = Instant.now();
    String creator = "creator";
    Integer tenantId = 1;
    String tenantName = "tenant";
    com.datastrato.unified_catalog.schema.SchemaVersion version =
        com.datastrato.unified_catalog.schema.SchemaVersion.V_0_1;
    Long lakehouseId = 1L;
    String lakehouseName = "lakehouse";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Long zoneId = 1L;
    String zoneName = "zone";
    Type columnType = TypeCreator.NULLABLE.STRING;

    com.datastrato.unified_catalog.schema.AuditInfo auditInfo =
        new com.datastrato.unified_catalog.schema.AuditInfo.Builder()
            .withCreator(creator)
            .withCreateTime(now)
            .build();

    // Test Tenant
    com.datastrato.unified_catalog.schema.Tenant tenant =
        new com.datastrato.unified_catalog.schema.Tenant.Builder()
            .withId(tenantId)
            .withName(tenantName)
            .withVersion(version)
            .withAuditInfo(auditInfo)
            .build();

    Tenant tenantProto = ProtoUtils.toProto(tenant);
    com.datastrato.unified_catalog.schema.Tenant tenantFromProto =
        ProtoUtils.fromProto(tenantProto);
    Assertions.assertEquals(tenant, tenantFromProto);

    byte[] tenantBytes = ProtoUtils.toBytes(tenant);
    com.datastrato.unified_catalog.schema.Tenant tenantFromBytes =
        ProtoUtils.fromBytes(
            tenantBytes, com.datastrato.unified_catalog.schema.Tenant.class, Tenant.parser());
    Assertions.assertEquals(tenant, tenantFromBytes);

    // Test Lakehouse
    com.datastrato.unified_catalog.schema.Lakehouse lakehouse =
        new com.datastrato.unified_catalog.schema.Lakehouse.Builder()
            .withId(lakehouseId)
            .withName(lakehouseName)
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();

    Lakehouse lakehouseProto = ProtoUtils.toProto(lakehouse);
    Assertions.assertEquals(props, lakehouseProto.getPropertiesMap());
    com.datastrato.unified_catalog.schema.Lakehouse lakehouseFromProto =
        ProtoUtils.fromProto(lakehouseProto);
    Assertions.assertEquals(lakehouse, lakehouseFromProto);

    byte[] lakehouseBytes = ProtoUtils.toBytes(lakehouse);
    com.datastrato.unified_catalog.schema.Lakehouse lakehouseFromBytes =
        ProtoUtils.fromBytes(
            lakehouseBytes,
            com.datastrato.unified_catalog.schema.Lakehouse.class,
            Lakehouse.parser());
    Assertions.assertEquals(lakehouse, lakehouseFromBytes);

    // Test Lakehouse without props map
    com.datastrato.unified_catalog.schema.Lakehouse lakehouse1 =
        new com.datastrato.unified_catalog.schema.Lakehouse.Builder()
            .withId(lakehouseId)
            .withName(lakehouseName)
            .withAuditInfo(auditInfo)
            .build();

    Lakehouse lakehouseProto1 = ProtoUtils.toProto(lakehouse1);
    Assertions.assertEquals(0, lakehouseProto1.getPropertiesCount());
    com.datastrato.unified_catalog.schema.Lakehouse lakehouseFromProto1 =
        ProtoUtils.fromProto(lakehouseProto1);
    Assertions.assertEquals(lakehouse1, lakehouseFromProto1);

    byte[] lakehouseBytes1 = ProtoUtils.toBytes(lakehouse1);
    com.datastrato.unified_catalog.schema.Lakehouse lakehouseFromBytes1 =
        ProtoUtils.fromBytes(
            lakehouseBytes1,
            com.datastrato.unified_catalog.schema.Lakehouse.class,
            Lakehouse.parser());
    Assertions.assertEquals(lakehouse1, lakehouseFromBytes1);

    // Test Zone
    com.datastrato.unified_catalog.schema.Zone zone =
        new com.datastrato.unified_catalog.schema.Zone.Builder()
            .withId(zoneId)
            .withLakehouseId(lakehouseId)
            .withName(zoneName)
            .withAuditInfo(auditInfo)
            .build();

    Zone zoneProto = ProtoUtils.toProto(zone);
    com.datastrato.unified_catalog.schema.Zone zoneFromProto = ProtoUtils.fromProto(zoneProto);
    Assertions.assertEquals(zone, zoneFromProto);

    byte[] zoneBytes = ProtoUtils.toBytes(zone);
    com.datastrato.unified_catalog.schema.Zone zoneFromBytes =
        ProtoUtils.fromBytes(
            zoneBytes, com.datastrato.unified_catalog.schema.Zone.class, Zone.parser());
    Assertions.assertEquals(zone, zoneFromBytes);

    // Test Column
    com.datastrato.unified_catalog.schema.Column column =
        new com.datastrato.unified_catalog.schema.Column.Builder()
            .withId(1)
            .withEntityId(1L)
            .withEntitySnapshotId(1L)
            .withName("col1")
            .withType(columnType)
            .withPosition(1)
            .withAuditInfo(auditInfo)
            .build();

    Column columnProto = ProtoUtils.toProto(column);
    com.datastrato.unified_catalog.schema.Column columnFromProto =
        ProtoUtils.fromProto(columnProto);
    Assertions.assertEquals(column, columnFromProto);
    Assertions.assertEquals(columnType, columnFromProto.getType());

    byte[] columnBytes = ProtoUtils.toBytes(column);
    com.datastrato.unified_catalog.schema.Column columnFromBytes =
        ProtoUtils.fromBytes(
            columnBytes, com.datastrato.unified_catalog.schema.Column.class, Column.parser());
    Assertions.assertEquals(column, columnFromBytes);
  }

  @Test
  public void testTableSerDe() {
    com.datastrato.unified_catalog.schema.Table.TableType tableType =
        com.datastrato.unified_catalog.schema.Table.TableType.VIRTUAL;
    com.datastrato.unified_catalog.schema.hasExtraInfo.ExtraInfo extraInfo =
        new com.datastrato.unified_catalog.schema.VirtualTableInfo(
            1, ImmutableList.of("db", "table"));
    com.datastrato.unified_catalog.schema.AuditInfo auditInfo =
        new com.datastrato.unified_catalog.schema.AuditInfo.Builder()
            .withCreator("creator")
            .withCreateTime(Instant.now())
            .build();

    com.datastrato.unified_catalog.schema.Table table =
        new com.datastrato.unified_catalog.schema.Table.Builder()
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

    com.datastrato.unified_catalog.schema.Table tableFromProto = ProtoUtils.fromProto(tableProto);
    Assertions.assertEquals(extraInfo, tableFromProto.getExtraInfo());
    Assertions.assertEquals(table, tableFromProto);

    byte[] tableBytes = ProtoUtils.toBytes(table);
    com.datastrato.unified_catalog.schema.Table tableFromBytes =
        ProtoUtils.fromBytes(
            tableBytes, com.datastrato.unified_catalog.schema.Table.class, Table.parser());
    Assertions.assertEquals(table, tableFromBytes);

    // Test Table with unsupported type
    com.datastrato.unified_catalog.schema.Table table1 =
        new com.datastrato.unified_catalog.schema.Table.Builder()
            .withId(1L)
            .withZoneId(1L)
            .withName("table")
            .withType(com.datastrato.unified_catalog.schema.Table.TableType.VIEW)
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
