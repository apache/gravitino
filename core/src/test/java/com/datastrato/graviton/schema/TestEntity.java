package com.datastrato.graviton.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEntity {
  private final Instant now = Instant.now();

  // Tenant test data
  private final Integer tenantId = 1;
  private final String tenantName = "testTenant";
  private final SchemaVersion version = SchemaVersion.V_0_1;
  private final AuditInfo auditInfo =
      new AuditInfo.Builder().withCreator("test").withCreateTime(now).build();

  // Lakehouse test data
  private final Long lakehouseId = 1L;
  private final String lakehouseName = "testLakehouse";
  private final Map<String, String> map = ImmutableMap.of("k1", "v1", "k2", "v2");

  // Zone test data
  private Long zoneId = 1L;
  private String zoneName = "testZone";

  // column test data
  private final Integer columnId = 1;
  private final String columnName = "testColumn";
  private final Long tableSnapshotId = 1L;
  private final Type columnType = TypeCreator.of(false).I8;
  private final Integer pos = 1;

  // table test data
  private final Long tableId = 1L;
  private final String tableName = "testTable";
  private final Table.TableType tableType = Table.TableType.VIRTUAL;
  private final hasExtraInfo.ExtraInfo extraInfo =
      new VirtualTableInfo(1, ImmutableList.of("test"));

  @Test
  public void testTenant() {
    Tenant tenant =
        new Tenant.Builder()
            .withId(tenantId)
            .withName(tenantName)
            .withAuditInfo(auditInfo)
            .withVersion(version)
            .build();

    Map<Field, Object> fields = tenant.fields();
    Assertions.assertEquals(tenantId, fields.get(Tenant.ID));
    Assertions.assertEquals(tenantName, fields.get(Tenant.NAME));
    Assertions.assertEquals(auditInfo, fields.get(Tenant.AUDIT_INFO));
    Assertions.assertEquals(version, fields.get(Tenant.VERSION));
    Assertions.assertNull(fields.get(Tenant.COMMENT));

    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> new Tenant.Builder().withId(tenantId).build());
    Assertions.assertTrue(exception.getMessage().matches("Field .* is required"));
  }

  @Test
  public void testLakehouse() {
    Lakehouse lakehouse =
        new Lakehouse.Builder()
            .withId(lakehouseId)
            .withName(lakehouseName)
            .withAuditInfo(auditInfo)
            .withProperties(map)
            .build();

    Map<Field, Object> fields = lakehouse.fields();
    Assertions.assertEquals(lakehouseId, fields.get(Lakehouse.ID));
    Assertions.assertEquals(lakehouseName, fields.get(Lakehouse.NAME));
    Assertions.assertEquals(map, fields.get(Lakehouse.PROPERTIES));
    Assertions.assertEquals(auditInfo, fields.get(Lakehouse.AUDIT_INFO));
    Assertions.assertNull(fields.get(Lakehouse.COMMENT));
  }

  @Test
  public void testZone() {
    Zone zone =
        new Zone.Builder()
            .withId(zoneId)
            .withLakehouseId(lakehouseId)
            .withName(zoneName)
            .withAuditInfo(auditInfo)
            .withProperties(map)
            .build();

    Map<Field, Object> fields = zone.fields();
    Assertions.assertEquals(zoneId, fields.get(Zone.ID));
    Assertions.assertEquals(lakehouseId, fields.get(Zone.LAKEHOUSE_ID));
    Assertions.assertEquals(zoneName, fields.get(Zone.NAME));
    Assertions.assertEquals(map, fields.get(Zone.PROPERTIES));
    Assertions.assertEquals(auditInfo, fields.get(Zone.AUDIT_INFO));
    Assertions.assertNull(fields.get(Zone.COMMENT));
  }

  @Test
  public void testColumn() {
    Column column =
        new Column.Builder()
            .withId(columnId)
            .withEntityId(tableId)
            .withEntitySnapshotId(tableSnapshotId)
            .withName(columnName)
            .withType(columnType)
            .withPosition(pos)
            .withAuditInfo(auditInfo)
            .build();

    Map<Field, Object> fields = column.fields();
    Assertions.assertEquals(columnId, fields.get(Column.ID));
    Assertions.assertEquals(tableId, fields.get(Column.ENTITY_ID));
    Assertions.assertEquals(tableSnapshotId, fields.get(Column.ENTITY_SNAPSHOT_ID));
    Assertions.assertEquals(columnName, fields.get(Column.NAME));
    Assertions.assertEquals(columnType, fields.get(Column.TYPE));
    Assertions.assertEquals(pos, fields.get(Column.POSITION));
    Assertions.assertEquals(auditInfo, fields.get(Column.AUDIT_INFO));
    Assertions.assertNull(fields.get(Column.COMMENT));
  }

  @Test
  public void testTable() {
    Table table =
        new Table.Builder()
            .withId(tableId)
            .withZoneId(zoneId)
            .withName(tableName)
            .withType(tableType)
            .withSnapshotId(tableSnapshotId)
            .withAuditInfo(auditInfo)
            .withExtraInfo(extraInfo)
            .build();

    Map<Field, Object> fields = table.fields();
    Assertions.assertEquals(tableId, fields.get(Table.ID));
    Assertions.assertEquals(zoneId, fields.get(Table.ZONE_ID));
    Assertions.assertEquals(tableName, fields.get(Table.NAME));
    Assertions.assertEquals(tableType, fields.get(Table.TABLE_TYPE));
    Assertions.assertEquals(tableSnapshotId, fields.get(Table.SNAPSHOT_ID));
    Assertions.assertEquals(auditInfo, fields.get(Table.AUDIT_INFO));
    Assertions.assertEquals(extraInfo, fields.get(Table.EXTRA_INFO));
    Assertions.assertNull(fields.get(Table.COMMENT));
    Assertions.assertNull(fields.get(Table.PROPERTIES));
  }
}
