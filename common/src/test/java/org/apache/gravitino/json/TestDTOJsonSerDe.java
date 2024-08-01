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
package org.apache.gravitino.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.EnumFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.rel.expressions.FieldReferenceDTO;
import org.apache.gravitino.dto.rel.expressions.FuncExpressionDTO;
import org.apache.gravitino.dto.rel.expressions.FunctionArg;
import org.apache.gravitino.dto.rel.expressions.LiteralDTO;
import org.apache.gravitino.dto.rel.partitioning.BucketPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.DayPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.FunctionPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.HourPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.IdentityPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.ListPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.MonthPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.Partitioning;
import org.apache.gravitino.dto.rel.partitioning.RangePartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.TruncatePartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.YearPartitioningDTO;
import org.apache.gravitino.dto.rel.partitions.ListPartitionDTO;
import org.apache.gravitino.dto.rel.partitions.RangePartitionDTO;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDTOJsonSerDe {

  private final String auditJson =
      "{\"creator\":%s,\"createTime\":%s,\"lastModifier\":%s,\"lastModifiedTime\":%s}";

  private final String metalakeJson = "{\"name\":%s,\"comment\":%s,\"properties\":%s,\"audit\":%s}";

  private final String columnJson =
      "{\"name\":%s,\"type\":%s,\"comment\":%s,\"nullable\":%s,\"autoIncrement\":%s}";

  private String getExpectedTableJson(
      String tableName,
      String tableComment,
      String columns,
      String properties,
      String audit,
      String distribution,
      String sortOrders,
      String partitioning,
      String indexes) {
    return String.format(
        "{\"name\":%s,\"comment\":%s,\"columns\":[%s],\"properties\":%s,\"audit\":%s,\"distribution\":%s,\"sortOrders\":%s,\"partitioning\":%s,\"indexes\":%s}",
        withQuotes(tableName),
        withQuotes(tableComment),
        columns,
        properties,
        audit,
        distribution,
        sortOrders,
        partitioning,
        indexes);
  }

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
        MetalakeDTO.builder()
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
    MetalakeDTO metalake1 = MetalakeDTO.builder().withName(name).withAudit(audit).build();

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
        CatalogDTO.builder()
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
        CatalogDTO.builder()
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
    Type type = Types.ByteType.get();
    String comment = "comment";

    // Test required fields
    ColumnDTO column =
        ColumnDTO.builder().withName(name).withDataType(type).withComment(comment).build();
    String serJson = JsonUtils.objectMapper().writeValueAsString(column);
    String expectedJson =
        String.format(
            columnJson,
            withQuotes(name),
            withQuotes(type.simpleString()),
            withQuotes(comment),
            column.nullable(),
            column.autoIncrement());
    Assertions.assertEquals(expectedJson, serJson);
    ColumnDTO deserColumn = JsonUtils.objectMapper().readValue(serJson, ColumnDTO.class);
    Assertions.assertEquals(column, deserColumn);
    Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, column.defaultValue());

    // test default nullable
    String json = "{\"name\":\"column\",\"type\":\"byte\",\"comment\":\"comment\"}";
    ColumnDTO deColumn = JsonUtils.objectMapper().readValue(json, ColumnDTO.class);
    Assertions.assertTrue(deColumn.nullable());

    // test specify column default value
    column =
        ColumnDTO.builder()
            .withName(name)
            .withDataType(Types.DateType.get())
            .withComment(comment)
            .withDefaultValue(
                LiteralDTO.builder()
                    .withDataType(Types.DateType.get())
                    .withValue("2023-04-01")
                    .build())
            .build();
    String actual = JsonUtils.objectMapper().writeValueAsString(column);
    String expected =
        "{\n"
            + "  \"name\": \"column\",\n"
            + "  \"type\": \"date\",\n"
            + "  \"comment\": \"comment\",\n"
            + "  \"nullable\": true,\n"
            + "  \"autoIncrement\": false,\n"
            + "  \"defaultValue\": {\n"
            + "    \"type\": \"literal\",\n"
            + "    \"dataType\": \"date\",\n"
            + "    \"value\": \"2023-04-01\"\n"
            + "  }\n"
            + "}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(actual));
  }

  @Test
  public void testTableDTOSerDe() throws Exception {
    String name = "column";
    Type type = Types.ByteType.get();
    String comment = "comment";
    String creator = "creator";
    Instant now = Instant.now();
    AuditDTO audit = AuditDTO.builder().withCreator(creator).withCreateTime(now).build();

    String tableName = "table";
    String tableComment = "comment";
    Map<String, String> properties = ImmutableMap.of("k1", "v1", "k2", "v2");

    ColumnDTO column =
        ColumnDTO.builder()
            .withName(name)
            .withDataType(type)
            .withComment(comment)
            .withNullable(false)
            .build();
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
        getExpectedTableJson(
            tableName,
            tableComment,
            String.format(
                columnJson,
                withQuotes(name),
                withQuotes(type.simpleString()),
                withQuotes(comment),
                column.nullable(),
                column.autoIncrement()),
            JsonUtils.objectMapper().writeValueAsString(properties),
            String.format(auditJson, withQuotes(creator), withQuotes(now.toString()), null, null),
            null,
            null,
            null,
            null);
    Assertions.assertEquals(expectedJson, serJson);
  }

  @Test
  public void testPartitioningDTOSerDe() throws Exception {

    String[] field1 = new String[] {"dt"};
    String[] field2 = new String[] {"city"};

    // construct simple partition
    Partitioning identity = IdentityPartitioningDTO.of(field1);
    Partitioning hourPart = HourPartitioningDTO.of(field1);
    Partitioning dayPart = DayPartitioningDTO.of(field1);
    Partitioning monthPart = MonthPartitioningDTO.of(field1);
    Partitioning yearPart = YearPartitioningDTO.of(field1);

    // construct list partition
    LiteralDTO[][] pCaliforniaValue = {
      {
        LiteralDTO.builder().withValue("2023-04-01").withDataType(Types.DateType.get()).build(),
        LiteralDTO.builder().withValue("San Francisco").withDataType(Types.StringType.get()).build()
      },
      {
        LiteralDTO.builder().withValue("2023-04-01").withDataType(Types.DateType.get()).build(),
        LiteralDTO.builder().withValue("San Diego").withDataType(Types.StringType.get()).build()
      }
    };
    ListPartitionDTO pCalifornia =
        ListPartitionDTO.builder()
            .withName("p202304_California")
            .withLists(pCaliforniaValue)
            .build();

    LiteralDTO[][] pTexasValue = {
      {
        LiteralDTO.builder().withValue("2023-04-01").withDataType(Types.DateType.get()).build(),
        LiteralDTO.builder().withValue("Houston").withDataType(Types.StringType.get()).build()
      },
      {
        LiteralDTO.builder().withValue("2023-04-01").withDataType(Types.DateType.get()).build(),
        LiteralDTO.builder().withValue("Dallas").withDataType(Types.StringType.get()).build()
      }
    };
    ListPartitionDTO pTexas =
        ListPartitionDTO.builder().withName("p202304_Texas").withLists(pTexasValue).build();

    Partitioning listPart =
        ListPartitioningDTO.of(
            new String[][] {field1, field2}, new ListPartitionDTO[] {pCalifornia, pTexas});

    // construct range partition
    RangePartitionDTO p20230101 =
        RangePartitionDTO.builder()
            .withName("p20230101")
            .withUpper(
                LiteralDTO.builder()
                    .withValue("2023-01-01")
                    .withDataType(Types.DateType.get())
                    .build())
            .withLower(
                LiteralDTO.builder()
                    .withValue("2023-01-01")
                    .withDataType(Types.DateType.get())
                    .build())
            .build();
    RangePartitionDTO p20230102 =
        RangePartitionDTO.builder()
            .withName("p20230102")
            .withUpper(
                LiteralDTO.builder()
                    .withValue("2023-01-02")
                    .withDataType(Types.DateType.get())
                    .build())
            .withLower(LiteralDTO.NULL)
            .build();
    Partitioning rangePart =
        RangePartitioningDTO.of(field1, new RangePartitionDTO[] {p20230101, p20230102});

    // construct function partitioning, toYYYYMM(toDate(ts, ‘Asia/Shanghai’))
    FunctionArg arg1 = FieldReferenceDTO.of(field1);
    FunctionArg arg2 =
        LiteralDTO.builder()
            .withDataType(Types.StringType.get())
            .withValue("Asia/Shanghai")
            .build();
    FunctionArg toDateFunc =
        FuncExpressionDTO.builder().withFunctionName("toDate").withFunctionArgs(arg1, arg2).build();
    Partitioning expressionPart = FunctionPartitioningDTO.of("toYYYYMM", toDateFunc);
    Partitioning bucketPart = BucketPartitioningDTO.of(10, field1);
    Partitioning truncatePart = TruncatePartitioningDTO.of(20, field2);

    Partitioning[] partitioning = {
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
            .writeValueAsString(partitioning);
    Partitioning[] desPartitioning =
        JsonUtils.objectMapper().readValue(serJson, Partitioning[].class);

    Assertions.assertArrayEquals(partitioning, desPartitioning);
  }

  @Test
  public void testPartitioningDTOSerDeFail() {
    // test `strategy` value null
    String wrongJson1 = "{\"strategy\": null,\"fieldName\":[\"dt\"]}";
    ObjectMapper map = JsonUtils.objectMapper();
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> map.readValue(wrongJson1, Partitioning.class));
    Assertions.assertTrue(
        illegalArgumentException
            .getMessage()
            .contains("Cannot parse to a string value strategy: null"));

    // test `fieldName` value empty
    String wrongJson2 = "{\"strategy\": \"day\",\"fieldName\":[]}";
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> map.readValue(wrongJson2, Partitioning.class));
    Assertions.assertTrue(exception.getMessage().contains("fieldName cannot be null or empty"));

    // test invalid `strategy` value
    String wrongJson6 = "{\"strategy\": \"my_strategy\"}";
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> map.readValue(wrongJson6, Partitioning.class));
    Assertions.assertTrue(exception.getMessage().contains("Invalid partitioning strategy"));

    // test invalid arguments for partitioning
    String wrongJson3 =
        "{\n"
            + "    \"strategy\": \"list\",\n"
            + "    \"fieldNames\": [\n"
            + "        [\n"
            + "            \"dt\"\n"
            + "        ],\n"
            + "        [\n"
            + "            \"city\"\n"
            + "        ]\n"
            + "    ],\n"
            + "    \"assignments\": \"partitions\"\n"
            + "}";
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> map.readValue(wrongJson3, Partitioning.class));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Cannot parse list partitioning from non-array assignments"),
        exception.getMessage());

    String wrong4 =
        "{\n"
            + "    \"strategy\": \"list\",\n"
            + "    \"fieldNames\": [\n"
            + "        [\n"
            + "            \"dt\"\n"
            + "        ],\n"
            + "        [\n"
            + "            \"city\"\n"
            + "        ]\n"
            + "    ],\n"
            + "    \"assignments\": [\n"
            + "        {\n"
            + "            \"type\": \"range\",\n"
            + "            \"name\": \"p20230101\",\n"
            + "            \"upper\": {\n"
            + "                \"type\": \"literal\",\n"
            + "                \"dataType\": \"date\",\n"
            + "                \"value\": \"2023-01-01\"\n"
            + "            },\n"
            + "            \"lower\": {\n"
            + "                \"type\": \"literal\",\n"
            + "                \"dataType\": \"date\",\n"
            + "                \"value\": \"2023-01-01\"\n"
            + "            },\n"
            + "            \"properties\": null\n"
            + "        }\n"
            + "    ]\n"
            + "}";
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> map.readValue(wrong4, Partitioning.class));
    Assertions.assertTrue(
        exception.getMessage().contains("Cannot parse list partitioning from non-list assignment"),
        exception.getMessage());
  }
}
