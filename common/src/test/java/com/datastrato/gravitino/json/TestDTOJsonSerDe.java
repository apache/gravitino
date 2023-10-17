/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.json;

import static com.datastrato.gravitino.dto.rel.ExpressionPartitionDTO.bucket;
import static com.datastrato.gravitino.dto.rel.ExpressionPartitionDTO.truncate;
import static com.datastrato.gravitino.dto.rel.SimplePartitionDTO.day;
import static com.datastrato.gravitino.dto.rel.SimplePartitionDTO.hour;
import static com.datastrato.gravitino.dto.rel.SimplePartitionDTO.identity;
import static com.datastrato.gravitino.dto.rel.SimplePartitionDTO.month;
import static com.datastrato.gravitino.dto.rel.SimplePartitionDTO.year;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.CatalogDTO;
import com.datastrato.gravitino.dto.MetalakeDTO;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.ExpressionPartitionDTO;
import com.datastrato.gravitino.dto.rel.ListPartitionDTO;
import com.datastrato.gravitino.dto.rel.Partition;
import com.datastrato.gravitino.dto.rel.RangePartitionDTO;
import com.datastrato.gravitino.dto.rel.TableDTO;
import com.fasterxml.jackson.databind.cfg.EnumFeature;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableMap;
import io.substrait.type.StringTypeVisitor;
import io.substrait.type.TypeCreator;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDTOJsonSerDe {

  private final String auditJson =
      "{\"creator\":%s,\"createTime\":%s,\"lastModifier\":%s,\"lastModifiedTime\":%s}";

  private final String metalakeJson = "{\"name\":%s,\"comment\":%s,\"properties\":%s,\"audit\":%s}";

  private final String columnJson = "{\"name\":%s,\"type\":%s,\"comment\":%s}";

  private final String tableJson =
      "{\"name\":%s,\"comment\":%s,\"columns\":[%s],\"properties\":%s,\"audit\":%s,\"distribution\":%s,\"sortOrders\":%s,\"partitions\":%s}";

  private String withQuotes(String str) {
    return "\"" + str + "\"";
  }

  @Test
  public void testAuditDTOJsonSerDe() throws Exception {
    Instant now = Instant.now();
    String creator = "creator";
    String modifier = "modifier";

    // Test with required fields
    AuditDTO audit = AuditDTO.builder().withCreator(creator).withCreateTime(now).build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(audit);
    String expectedJson =
        String.format(auditJson, withQuotes(creator), withQuotes(now.toString()), null, null);
    Assertions.assertEquals(expectedJson, serJson);
    AuditDTO deserAudit = JsonUtils.objectMapper().readValue(serJson, AuditDTO.class);
    Assertions.assertEquals(audit, deserAudit);

    // Test with optional fields
    AuditDTO audit1 =
        AuditDTO.builder()
            .withCreator(creator)
            .withCreateTime(now)
            .withLastModifier(modifier)
            .withLastModifiedTime(now)
            .build();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(audit1);
    String expectedJson1 =
        String.format(
            auditJson,
            withQuotes(creator),
            withQuotes(now.toString()),
            withQuotes(modifier),
            withQuotes(now.toString()));
    Assertions.assertEquals(expectedJson1, serJson1);
    AuditDTO deserAudit1 = JsonUtils.objectMapper().readValue(serJson1, AuditDTO.class);
    Assertions.assertEquals(audit1, deserAudit1);
  }

  @Test
  public void testMetalakeDTOSerDe() throws Exception {
    String name = "metalake";
    String comment = "comment";
    Map<String, String> properties = ImmutableMap.of("k1", "v1", "k2", "v2");
    String creator = "creator";
    Instant now = Instant.now();
    AuditDTO audit = AuditDTO.builder().withCreator(creator).withCreateTime(now).build();

    // Test with required fields
    MetalakeDTO metalake =
        new MetalakeDTO.Builder()
            .withName(name)
            .withComment(comment)
            .withProperties(properties)
            .withAudit(audit)
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(metalake);
    String expectedJson =
        String.format(
            metalakeJson,
            withQuotes(name),
            withQuotes(comment),
            JsonUtils.objectMapper().writeValueAsString(properties),
            String.format(auditJson, withQuotes(creator), withQuotes(now.toString()), null, null));
    Assertions.assertEquals(expectedJson, serJson);
    MetalakeDTO desermetalake = JsonUtils.objectMapper().readValue(serJson, MetalakeDTO.class);
    Assertions.assertEquals(metalake, desermetalake);

    // Test with optional fields
    MetalakeDTO metalake1 = new MetalakeDTO.Builder().withName(name).withAudit(audit).build();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(metalake1);
    String expectedJson1 =
        String.format(
            metalakeJson,
            withQuotes(name),
            null,
            null,
            String.format(auditJson, withQuotes(creator), withQuotes(now.toString()), null, null));
    Assertions.assertEquals(expectedJson1, serJson1);
    MetalakeDTO desermetalake1 = JsonUtils.objectMapper().readValue(serJson1, MetalakeDTO.class);
    Assertions.assertEquals(metalake1, desermetalake1);
  }

  @Test
  public void testCatalogDTOSerDe() throws Exception {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    CatalogDTO catalog =
        new CatalogDTO.Builder()
            .withName("catalog")
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withComment("comment")
            .withProperties(ImmutableMap.of("k1", "v1", "k2", "v2"))
            .withAudit(audit)
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(catalog);
    CatalogDTO deserCatalog = JsonUtils.objectMapper().readValue(serJson, CatalogDTO.class);
    Assertions.assertEquals(catalog, deserCatalog);

    // test with optional fields
    CatalogDTO catalog1 =
        new CatalogDTO.Builder()
            .withName("catalog")
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAudit(audit)
            .build();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(catalog1);
    CatalogDTO deserCatalog1 = JsonUtils.objectMapper().readValue(serJson1, CatalogDTO.class);
    Assertions.assertEquals(catalog1, deserCatalog1);
  }

  @Test
  public void testColumnDTOSerDe() throws Exception {
    String name = "column";
    io.substrait.type.Type type = TypeCreator.NULLABLE.I8;
    String comment = "comment";

    // Test required fields
    ColumnDTO column =
        ColumnDTO.builder().withName(name).withDataType(type).withComment(comment).build();
    String serJson = JsonUtils.objectMapper().writeValueAsString(column);
    String expectedJson =
        String.format(
            columnJson,
            withQuotes(name),
            withQuotes(type.accept(new StringTypeVisitor())),
            withQuotes(comment));
    Assertions.assertEquals(expectedJson, serJson);
    ColumnDTO deserColumn = JsonUtils.objectMapper().readValue(serJson, ColumnDTO.class);
    Assertions.assertEquals(column, deserColumn);
  }

  @Test
  public void testTableDTOSerDe() throws Exception {
    String name = "column";
    io.substrait.type.Type type = TypeCreator.NULLABLE.I8;
    String comment = "comment";
    String creator = "creator";
    Instant now = Instant.now();
    AuditDTO audit = AuditDTO.builder().withCreator(creator).withCreateTime(now).build();

    String tableName = "table";
    String tableComment = "comment";
    Map<String, String> properties = ImmutableMap.of("k1", "v1", "k2", "v2");

    ColumnDTO column =
        ColumnDTO.builder().withName(name).withDataType(type).withComment(comment).build();
    TableDTO table =
        TableDTO.builder()
            .withName(tableName)
            .withComment(tableComment)
            .withColumns(new ColumnDTO[] {column})
            .withProperties(properties)
            .withAudit(audit)
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(table);
    String expectedJson =
        String.format(
            tableJson,
            withQuotes(tableName),
            withQuotes(tableComment),
            String.format(
                columnJson,
                withQuotes(name),
                withQuotes(type.accept(new StringTypeVisitor())),
                withQuotes(comment)),
            JsonUtils.objectMapper().writeValueAsString(properties),
            String.format(auditJson, withQuotes(creator), withQuotes(now.toString()), null, null),
            null,
            null,
            null);
    Assertions.assertEquals(expectedJson, serJson);
  }

  @Test
  public void testPartitionDTOSerDe() throws Exception {

    String[] field1 = new String[] {"dt"};
    String[] field2 = new String[] {"city"};

    // construct simple partition
    Partition identity = identity(field1);
    Partition hourPart = hour(field1);
    Partition dayPart = day(field1);
    Partition monthPart = month(field1);
    Partition yearPart = year(field1);

    // construct list partition
    String[][] p1Value = {{"2023-04-01", "San Francisco"}, {"2023-04-01", "San Francisco"}};
    String[][] p2Value = {{"2023-04-01", "Houston"}, {"2023-04-01", "Dallas"}};
    Partition listPart =
        new ListPartitionDTO.Builder()
            .withFieldNames(new String[][] {field1, field2})
            .withAssignment("p202304_California", p1Value)
            .withAssignment("p202304_Texas", p2Value)
            .build();

    // construct range partition
    Partition rangePart =
        new RangePartitionDTO.Builder()
            .withFieldName(field1)
            .withRange("p20230101", "2023-01-01T00:00:00", "2023-01-02T00:00:00")
            .withRange("p20230102", "2023-01-01T00:00:00", null)
            .build();

    // construct expression partition, toYYYYMM(toDate(ts, ‘Asia/Shanghai’))
    ExpressionPartitionDTO.Expression arg1 =
        new ExpressionPartitionDTO.FieldExpression.Builder().withFieldName(field1).build();
    ExpressionPartitionDTO.Expression arg2 =
        new ExpressionPartitionDTO.LiteralExpression.Builder()
            .withType(TypeCreator.REQUIRED.STRING)
            .withValue("Asia/Shanghai")
            .build();
    ExpressionPartitionDTO.Expression toDateFunc =
        new ExpressionPartitionDTO.FunctionExpression.Builder()
            .withFuncName("toDate")
            .withArgs(new ExpressionPartitionDTO.Expression[] {arg1, arg2})
            .build();
    ExpressionPartitionDTO.Expression monthFunc =
        new ExpressionPartitionDTO.FunctionExpression.Builder()
            .withFuncName("toYYYYMM")
            .withArgs(new ExpressionPartitionDTO.Expression[] {toDateFunc})
            .build();
    Partition expressionPart = new ExpressionPartitionDTO.Builder(monthFunc).build();
    Partition bucketPart = bucket(field1, 10);
    Partition truncatePart = truncate(field2, 20);

    Partition[] partitions = {
      identity,
      hourPart,
      dayPart,
      monthPart,
      yearPart,
      listPart,
      rangePart,
      expressionPart,
      bucketPart,
      truncatePart
    };
    String serJson =
        JsonMapper.builder()
            .configure(EnumFeature.WRITE_ENUMS_TO_LOWERCASE, true)
            .build()
            .writeValueAsString(partitions);
    Partition[] desPartitions = JsonUtils.objectMapper().readValue(serJson, Partition[].class);

    Assertions.assertArrayEquals(partitions, desPartitions);
  }

  @Test
  public void testPartitionDTOSerDeFail() throws Exception {
    // test `strategy` value null
    String wrongJson1 = "{\"strategy\": null,\"fieldName\":[\"dt\"]}";
    InvalidTypeIdException invalidTypeIdException =
        Assertions.assertThrows(
            InvalidTypeIdException.class,
            () -> JsonUtils.objectMapper().readValue(wrongJson1, Partition.class));
    Assertions.assertTrue(
        invalidTypeIdException.getMessage().contains("missing type id property 'strategy'"));

    // test `fieldName` value empty
    String wrongJson2 = "{\"strategy\": \"day\",\"fieldName\":[]}";
    ValueInstantiationException valueInstantiationException =
        Assertions.assertThrows(
            ValueInstantiationException.class,
            () -> JsonUtils.objectMapper().readValue(wrongJson2, Partition.class));
    Assertions.assertTrue(
        valueInstantiationException.getMessage().contains("fieldName cannot be null or empty"));

    // test listPartition assignment values missing
    String wrongJson3 =
        "{\"strategy\": \"list\",\"fieldNames\":[[\"dt\"],[\"city\"]],"
            + "\"assignments\":["
            + "{\"name\":\"p202304_California\", "
            + "\"values\":[[\"2023-04-01\",\"San Francisco\"], [\"2023-04-01\"]]}]}";
    valueInstantiationException =
        Assertions.assertThrows(
            ValueInstantiationException.class,
            () -> JsonUtils.objectMapper().readValue(wrongJson3, Partition.class));
    Assertions.assertTrue(
        valueInstantiationException
            .getMessage()
            .contains("Assignment values length must be equal to field number"));

    // test rangePartition range name missing
    String wrongJson4 =
        "{\"strategy\": \"range\",\"fieldName\":[\"dt\"],"
            + "\"ranges\":["
            + "{\"lower\":null, \"upper\":\"2023-01-02T00:00:00\"},"
            + "{\"lower\":\"2023-01-01T00:00:00\", \"upper\":null}]}";
    valueInstantiationException =
        Assertions.assertThrows(
            ValueInstantiationException.class,
            () -> JsonUtils.objectMapper().readValue(wrongJson4, Partition.class));
    Assertions.assertTrue(
        valueInstantiationException.getMessage().contains("Range name cannot be null or empty"));

    // test rangePartition range bound literal format wrong
    String wrongJson5 =
        "{\"strategy\": \"range\",\"fieldName\":[\"dt\"],"
            + "\"ranges\":["
            + "{\"name\":\"p20230101\", \"lower\":\"2023-01-01T00:00:00\", \"upper\":\"2023-01-02\"},"
            + "{\"name\":\"p20230102\", \"lower\":\"2023-01-01T00:00:00\", \"upper\":null}]}";
    valueInstantiationException =
        Assertions.assertThrows(
            ValueInstantiationException.class,
            () -> JsonUtils.objectMapper().readValue(wrongJson5, Partition.class));
    Assertions.assertTrue(
        valueInstantiationException
            .getMessage()
            .contains("only supports ISO date-time format literal"));

    // test expressionPartition `expression` value null
    String wrongJson6 = "{\"strategy\": \"expression\", \"expression\": null}";
    valueInstantiationException =
        Assertions.assertThrows(
            ValueInstantiationException.class,
            () -> JsonUtils.objectMapper().readValue(wrongJson6, Partition.class));
    Assertions.assertTrue(
        valueInstantiationException.getMessage().contains("expression cannot be null"));

    // test expressionPartition with FunctionExpression `funcName` value empty
    String wrongJson7 =
        "{\"strategy\": \"expression\", \"expression\": {\"funcName\": \"\", \"args\": []}}";
    valueInstantiationException =
        Assertions.assertThrows(
            ValueInstantiationException.class,
            () -> JsonUtils.objectMapper().readValue(wrongJson7, Partition.class));
    Assertions.assertTrue(
        valueInstantiationException.getMessage().contains("funcName cannot be null or empty"));
  }
}
