package com.datastrato.unified_catalog.schema.json;

import com.datastrato.unified_catalog.schema.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.substrait.type.StringTypeVisitor;
import io.substrait.type.TypeCreator;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEntityJsonSerDe {

  private final String auditInfoJson =
      "{\"creator\":%s,\"create_time\":%s,"
          + "\"last_modifier\":%s,\"last_modified_time\":%s,"
          + "\"last_access_user\":%s,\"last_access_time\":%s}";

  private final String tenantJson =
      "{\"id\":%d,\"name\":%s,\"comment\":%s,"
          + "\"audit_info\":%s,\"version\":{\"major_version\":%d,\"minor_version\":%d}}";

  private final String lakehouseJson =
      "{\"id\":%d,\"name\":%s,\"comment\":%s,\"properties\":%s,\"audit_info\":%s}";

  private final String zoneJson =
      "{\"id\":%d,\"lakehouse_id\":%d,\"name\":%s,\"comment\":%s,"
          + "\"properties\":%s,\"audit_info\":%s}";

  private final String columnJson =
      "{\"id\":%d,\"entity_id\":%d,\"entity_snapshot_id\":%d,\"name\":%s,"
          + "\"type\":%s,\"comment\":%s,\"position\":%d,\"audit_info\":%s}";

  private final String tableJson =
      "{\"id\":%d,\"zone_id\":%d,\"name\":%s,\"comment\":%s,\"type\":%s,\"snapshot_id\":%d,"
          + "\"properties\":%s,\"audit_info\":%s,"
          + "\"extra_info\":{\"type\":%s,\"connection_id\":%d,\"identifier\":%s},"
          + "\"columns\":[%s]}";

  private String withQuotes(String str) {
    return "\"" + str + "\"";
  }

  @Test
  public void testAuditInfoJsonSerDe() throws Exception {
    Instant now = Instant.now();
    String creator = "creator";
    String modifier = "modifier";
    String accessor = "accessor";

    // Test with required fields
    AuditInfo info = new AuditInfo.Builder().withCreator(creator).withCreateTime(now).build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(info);
    String expectedJson =
        String.format(
            auditInfoJson, withQuotes(creator), withQuotes(now.toString()), null, null, null, null);
    Assertions.assertEquals(expectedJson, serJson);
    AuditInfo deserInfo = JsonUtils.objectMapper().readValue(serJson, AuditInfo.class);
    Assertions.assertEquals(info, deserInfo);

    // Test with optional fields
    AuditInfo info1 =
        new AuditInfo.Builder()
            .withCreator(creator)
            .withCreateTime(now)
            .withLastModifier(modifier)
            .withLastModifiedTime(now)
            .build();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(info1);
    String expectedJson1 =
        String.format(
            auditInfoJson,
            withQuotes(creator),
            withQuotes(now.toString()),
            withQuotes(modifier),
            withQuotes(now.toString()),
            null,
            null);
    Assertions.assertEquals(expectedJson1, serJson1);
    AuditInfo deserInfo1 = JsonUtils.objectMapper().readValue(serJson1, AuditInfo.class);
    Assertions.assertEquals(info1, deserInfo1);

    AuditInfo info2 =
        new AuditInfo.Builder()
            .withCreator(creator)
            .withCreateTime(now)
            .withLastModifier(modifier)
            .withLastModifiedTime(now)
            .withLastAccessUser(accessor)
            .withLastAccessTime(now)
            .build();

    String serJson2 = JsonUtils.objectMapper().writeValueAsString(info2);
    String expectedJson2 =
        String.format(
            auditInfoJson,
            withQuotes(creator),
            withQuotes(now.toString()),
            withQuotes(modifier),
            withQuotes(now.toString()),
            withQuotes(accessor),
            withQuotes(now.toString()));
    Assertions.assertEquals(expectedJson2, serJson2);
    AuditInfo deserInfo2 = JsonUtils.objectMapper().readValue(serJson2, AuditInfo.class);
    Assertions.assertEquals(info2, deserInfo2);
  }

  @Test
  public void testTenantSerDe() throws Exception {
    Integer id = 1;
    String name = "tenant";
    String comment = "comment";
    String creator = "creator";
    Instant now = Instant.now();
    AuditInfo info = new AuditInfo.Builder().withCreator(creator).withCreateTime(now).build();
    SchemaVersion version = SchemaVersion.V_0_1;

    // Test with required fields
    Tenant tenant =
        new Tenant.Builder()
            .withId(id)
            .withName(name)
            .withComment(comment)
            .withAuditInfo(info)
            .withVersion(version)
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(tenant);
    String expectedJson =
        String.format(
            tenantJson,
            id,
            withQuotes(name),
            withQuotes(comment),
            String.format(
                auditInfoJson,
                withQuotes(creator),
                withQuotes(now.toString()),
                null,
                null,
                null,
                null),
            version.getMajorVersion(),
            version.getMinorVersion());
    Assertions.assertEquals(expectedJson, serJson);
    Tenant deserTenant = JsonUtils.objectMapper().readValue(serJson, Tenant.class);
    Assertions.assertEquals(tenant, deserTenant);

    // Test with optional fields
    Tenant tenant1 =
        new Tenant.Builder()
            .withId(id)
            .withName(name)
            .withAuditInfo(info)
            .withVersion(version)
            .build();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(tenant1);
    String expectedJson1 =
        String.format(
            tenantJson,
            id,
            withQuotes(name),
            null,
            String.format(
                auditInfoJson,
                withQuotes(creator),
                withQuotes(now.toString()),
                null,
                null,
                null,
                null),
            version.getMajorVersion(),
            version.getMinorVersion());
    Assertions.assertEquals(expectedJson1, serJson1);
    Tenant deserTenant1 = JsonUtils.objectMapper().readValue(serJson1, Tenant.class);
    Assertions.assertEquals(tenant1, deserTenant1);
  }

  @Test
  public void testLakehouseSerDe() throws Exception {
    Long id = 1L;
    String name = "lakehouse";
    String comment = "comment";
    Map<String, String> properties = ImmutableMap.of("k1", "v1", "k2", "v2");
    String creator = "creator";
    Instant now = Instant.now();
    AuditInfo info = new AuditInfo.Builder().withCreator(creator).withCreateTime(now).build();

    // Test with required fields
    Lakehouse lakehouse =
        new Lakehouse.Builder()
            .withId(id)
            .withName(name)
            .withComment(comment)
            .withProperties(properties)
            .withAuditInfo(info)
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(lakehouse);
    String expectedJson =
        String.format(
            lakehouseJson,
            id,
            withQuotes(name),
            withQuotes(comment),
            JsonUtils.objectMapper().writeValueAsString(properties),
            String.format(
                auditInfoJson,
                withQuotes(creator),
                withQuotes(now.toString()),
                null,
                null,
                null,
                null));
    Assertions.assertEquals(expectedJson, serJson);
    Lakehouse deserLakehouse = JsonUtils.objectMapper().readValue(serJson, Lakehouse.class);
    Assertions.assertEquals(lakehouse, deserLakehouse);

    // Test with optional fields
    Lakehouse lakehouse1 =
        new Lakehouse.Builder().withId(id).withName(name).withAuditInfo(info).build();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(lakehouse1);
    String expectedJson1 =
        String.format(
            lakehouseJson,
            id,
            withQuotes(name),
            null,
            null,
            String.format(
                auditInfoJson,
                withQuotes(creator),
                withQuotes(now.toString()),
                null,
                null,
                null,
                null));
    Assertions.assertEquals(expectedJson1, serJson1);
    Lakehouse deserLakehouse1 = JsonUtils.objectMapper().readValue(serJson1, Lakehouse.class);
    Assertions.assertEquals(lakehouse1, deserLakehouse1);
  }

  @Test
  public void testZoneSerDe() throws Exception {
    Long id = 1L;
    Long lakehouseId = 2L;
    String name = "zone";
    String comment = "comment";
    String creator = "creator";
    Instant now = Instant.now();
    AuditInfo info = new AuditInfo.Builder().withCreator(creator).withCreateTime(now).build();
    Map<String, String> properties = ImmutableMap.of("k1", "v1", "k2", "v2");

    // Test with required fields
    Zone zone =
        new Zone.Builder()
            .withId(id)
            .withLakehouseId(lakehouseId)
            .withName(name)
            .withComment(comment)
            .withProperties(properties)
            .withAuditInfo(info)
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(zone);
    String expectedJson =
        String.format(
            zoneJson,
            id,
            lakehouseId,
            withQuotes(name),
            withQuotes(comment),
            JsonUtils.objectMapper().writeValueAsString(properties),
            String.format(
                auditInfoJson,
                withQuotes(creator),
                withQuotes(now.toString()),
                null,
                null,
                null,
                null));
    Assertions.assertEquals(expectedJson, serJson);
    Zone deserZone = JsonUtils.objectMapper().readValue(serJson, Zone.class);
    Assertions.assertEquals(zone, deserZone);

    // Test with optional fields
    Zone zone1 =
        new Zone.Builder()
            .withId(id)
            .withLakehouseId(lakehouseId)
            .withName(name)
            .withAuditInfo(info)
            .build();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(zone1);
    String expectedJson1 =
        String.format(
            zoneJson,
            id,
            lakehouseId,
            withQuotes(name),
            null,
            null,
            String.format(
                auditInfoJson,
                withQuotes(creator),
                withQuotes(now.toString()),
                null,
                null,
                null,
                null));
    Assertions.assertEquals(expectedJson1, serJson1);
    Zone deserZone1 = JsonUtils.objectMapper().readValue(serJson1, Zone.class);
    Assertions.assertEquals(zone1, deserZone1);
    System.out.println(deserZone);
    System.out.println(deserZone1);
  }

  @Test
  public void testColumnSerDe() throws Exception {
    Integer id = 1;
    Long entityId = 1L;
    Long entitySnapshotId = 2L;
    String name = "column";
    io.substrait.type.Type type = TypeCreator.NULLABLE.I8;
    String comment = "comment";
    Integer position = 1;
    String creator = "creator";
    Instant now = Instant.now();
    AuditInfo info = new AuditInfo.Builder().withCreator(creator).withCreateTime(now).build();

    // Test required fields
    Column column =
        new Column.Builder()
            .withId(id)
            .withEntityId(entityId)
            .withEntitySnapshotId(entitySnapshotId)
            .withName(name)
            .withType(type)
            .withComment(comment)
            .withPosition(position)
            .withAuditInfo(info)
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(column);
    String expectedJson =
        String.format(
            columnJson,
            id,
            entityId,
            entitySnapshotId,
            withQuotes(name),
            withQuotes(type.accept(new StringTypeVisitor())),
            withQuotes(comment),
            position,
            String.format(
                auditInfoJson,
                withQuotes(creator),
                withQuotes(now.toString()),
                null,
                null,
                null,
                null));
    Assertions.assertEquals(expectedJson, serJson);
    Column deserColumn = JsonUtils.objectMapper().readValue(serJson, Column.class);
    Assertions.assertEquals(column, deserColumn);
  }

  @Test
  public void testTableSerDe() throws Exception {
    Integer columnId = 1;
    Long tableId = 1L;
    Long tableSnapshotId = 2L;
    String name = "column";
    io.substrait.type.Type type = TypeCreator.NULLABLE.I8;
    String comment = "comment";
    Integer position = 1;
    String creator = "creator";
    Instant now = Instant.now();
    AuditInfo info = new AuditInfo.Builder().withCreator(creator).withCreateTime(now).build();

    Long zoneId = 2L;
    String tableName = "table";
    String tableComment = "comment";
    Table.TableType tableType = Table.TableType.VIRTUAL;
    Map<String, String> properties = ImmutableMap.of("k1", "v1", "k2", "v2");

    Integer connectionId = 1;
    List<String> identifiers = ImmutableList.of("db1", "tabl1");
    hasExtraInfo.ExtraInfo extraInfo = new VirtualTableInfo(connectionId, identifiers);

    Column column =
        new Column.Builder()
            .withId(columnId)
            .withEntityId(tableId)
            .withEntitySnapshotId(tableSnapshotId)
            .withName(name)
            .withType(type)
            .withComment(comment)
            .withPosition(position)
            .withAuditInfo(info)
            .build();

    Table table =
        new Table.Builder()
            .withId(tableId)
            .withZoneId(zoneId)
            .withName(tableName)
            .withComment(tableComment)
            .withSnapshotId(tableSnapshotId)
            .withProperties(properties)
            .withExtraInfo(extraInfo)
            .withAuditInfo(info)
            .withType(tableType)
            .withColumns(ImmutableList.of(column))
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(table);
    String expectedJson =
        String.format(
            tableJson,
            tableId,
            zoneId,
            withQuotes(tableName),
            withQuotes(tableComment),
            withQuotes(tableType.getTypeStr()),
            tableSnapshotId,
            JsonUtils.objectMapper().writeValueAsString(properties),
            String.format(
                auditInfoJson,
                withQuotes(creator),
                withQuotes(now.toString()),
                null,
                null,
                null,
                null),
            withQuotes(tableType.getTypeStr()),
            connectionId,
            JsonUtils.objectMapper().writeValueAsString(identifiers),
            String.format(
                columnJson,
                columnId,
                tableId,
                tableSnapshotId,
                withQuotes(name),
                withQuotes(type.accept(new StringTypeVisitor())),
                withQuotes(comment),
                position,
                String.format(
                    auditInfoJson,
                    withQuotes(creator),
                    withQuotes(now.toString()),
                    null,
                    null,
                    null,
                    null)));
    Assertions.assertEquals(expectedJson, serJson);

    Table deserTable = JsonUtils.objectMapper().readValue(serJson, Table.class);
    Assertions.assertEquals(table, deserTable);
  }
}
