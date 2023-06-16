package com.datastrato.graviton.json;

import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.meta.*;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.catalog.rel.Column;
import com.datastrato.graviton.meta.catalog.rel.Table;
import com.google.common.collect.ImmutableMap;
import io.substrait.type.StringTypeVisitor;
import io.substrait.type.TypeCreator;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.*;

public class TestEntityJsonSerDe {

  private final String auditInfoJson =
      "{\"creator\":%s,\"create_time\":%s,\"last_modifier\":%s,\"last_modified_time\":%s}";

  private final String lakehouseJson =
      "{\"id\":%d,\"name\":%s,\"comment\":%s,\"properties\":%s,\"audit_info\":%s,"
          + "\"version\":{\"major_version\":%d,\"minor_version\":%d}}";

  private final String testColumnJson =
      "{\"name\":%s,\"comment\":%s,\"type\":%s,\"audit_info\":%s}";

  private final String testTableJson =
      "{\"name\":%s,\"comment\":%s,\"properties\":%s,\"audit_info\":%s,\"columns\":[%s]}";

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
        String.format(auditInfoJson, withQuotes(creator), withQuotes(now.toString()), null, null);
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
            withQuotes(now.toString()));
    Assertions.assertEquals(expectedJson1, serJson1);
    AuditInfo deserInfo1 = JsonUtils.objectMapper().readValue(serJson1, AuditInfo.class);
    Assertions.assertEquals(info1, deserInfo1);
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
    SchemaVersion version = SchemaVersion.V_0_1;

    // Test with required fields
    Lakehouse lakehouse =
        new Lakehouse.Builder()
            .withId(id)
            .withName(name)
            .withComment(comment)
            .withProperties(properties)
            .withAuditInfo(info)
            .withVersion(version)
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
                auditInfoJson, withQuotes(creator), withQuotes(now.toString()), null, null),
            version.getMajorVersion(),
            version.getMinorVersion());
    Assertions.assertEquals(expectedJson, serJson);
    Lakehouse deserLakehouse = JsonUtils.objectMapper().readValue(serJson, Lakehouse.class);
    Assertions.assertEquals(lakehouse, deserLakehouse);

    // Test with optional fields
    Lakehouse lakehouse1 =
        new Lakehouse.Builder()
            .withId(id)
            .withName(name)
            .withAuditInfo(info)
            .withVersion(version)
            .build();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(lakehouse1);
    String expectedJson1 =
        String.format(
            lakehouseJson,
            id,
            withQuotes(name),
            null,
            null,
            String.format(
                auditInfoJson, withQuotes(creator), withQuotes(now.toString()), null, null),
            version.getMajorVersion(),
            version.getMinorVersion());
    Assertions.assertEquals(expectedJson1, serJson1);
    Lakehouse deserLakehouse1 = JsonUtils.objectMapper().readValue(serJson1, Lakehouse.class);
    Assertions.assertEquals(lakehouse1, deserLakehouse1);
  }

  @Test
  public void testColumnSerDe() throws Exception {
    String name = "column";
    io.substrait.type.Type type = TypeCreator.NULLABLE.I8;
    String comment = "comment";
    String creator = "creator";
    Instant now = Instant.now();
    AuditInfo info = new AuditInfo.Builder().withCreator(creator).withCreateTime(now).build();

    // Test required fields
    Column column = new TestColumn(name, comment, type, info);
    String serJson = JsonUtils.objectMapper().writeValueAsString(column);
    String expectedJson =
        String.format(
            testColumnJson,
            withQuotes(name),
            withQuotes(comment),
            withQuotes(type.accept(new StringTypeVisitor())),
            String.format(
                auditInfoJson, withQuotes(creator), withQuotes(now.toString()), null, null),
            type.nullable());
    Assertions.assertEquals(expectedJson, serJson);
    Column deserColumn = JsonUtils.objectMapper().readValue(serJson, TestColumn.class);
    Assertions.assertEquals(column, deserColumn);
  }

  @Test
  public void testTableSerDe() throws Exception {
    String name = "column";
    io.substrait.type.Type type = TypeCreator.NULLABLE.I8;
    String comment = "comment";
    String creator = "creator";
    Instant now = Instant.now();
    AuditInfo info = new AuditInfo.Builder().withCreator(creator).withCreateTime(now).build();

    String tableName = "table";
    String tableComment = "comment";
    Map<String, String> properties = ImmutableMap.of("k1", "v1", "k2", "v2");

    Column column = new TestColumn(name, comment, type, info);
    Namespace namespace = Namespace.of("catalog", "db");
    Table table =
        new TestTable(tableName, namespace, tableComment, properties, info, new Column[] {column});

    String serJson = JsonUtils.objectMapper().writeValueAsString(table);
    String expectedJson =
        String.format(
            testTableJson,
            withQuotes(tableName),
            withQuotes(tableComment),
            JsonUtils.objectMapper().writeValueAsString(properties),
            String.format(
                auditInfoJson, withQuotes(creator), withQuotes(now.toString()), null, null),
            String.format(
                testColumnJson,
                withQuotes(name),
                withQuotes(comment),
                withQuotes(type.accept(new StringTypeVisitor())),
                String.format(
                    auditInfoJson, withQuotes(creator), withQuotes(now.toString()), null, null)));
    Assertions.assertEquals(expectedJson, serJson);
  }
}
