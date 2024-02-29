/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.datastrato.gravitino.dto.rel.expressions.LiteralDTO;
import com.datastrato.gravitino.dto.rel.indexes.IndexDTO;
import com.datastrato.gravitino.dto.rel.partitions.IdentityPartitionDTO;
import com.datastrato.gravitino.dto.rel.partitions.ListPartitionDTO;
import com.datastrato.gravitino.dto.rel.partitions.PartitionDTO;
import com.datastrato.gravitino.dto.rel.partitions.RangePartitionDTO;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestJsonUtils {

  private static ObjectMapper objectMapper;

  @BeforeAll
  static void setUp() {
    objectMapper = JsonUtils.objectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(Type.class, new JsonUtils.TypeSerializer());
    module.addDeserializer(Type.class, new JsonUtils.TypeDeserializer());
    objectMapper.registerModule(module);
  }

  @Test
  void testGetString() throws Exception {
    String json = "{\"property\": \"value\"}";
    JsonNode node = objectMapper.readTree(json);

    String result = JsonUtils.getString("property", node);
    assertEquals("value", result);
  }

  @Test
  void testGetStringListOrNull() throws Exception {
    String json = "{\"property\": [\"value1\", \"value2\"]}";
    JsonNode node = objectMapper.readTree(json);

    List<String> result = JsonUtils.getStringListOrNull("property", node);
    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals("value1", result.get(0));
    assertEquals("value2", result.get(1));

    result = JsonUtils.getStringListOrNull("unknown", node);
    assertNull(result);

    result = JsonUtils.getStringListOrNull("unknown", node);
    assertNull(result);
  }

  @Test
  public void testTypeSerDe() throws Exception {
    Type type = Types.BooleanType.get();
    String jsonValue = JsonUtils.objectMapper().writeValueAsString(type);
    String expected = "\"boolean\"";
    Assertions.assertEquals(objectMapper.readTree(expected), objectMapper.readTree(jsonValue));

    type = Types.TimestampType.withTimeZone();
    jsonValue = JsonUtils.objectMapper().writeValueAsString(type);
    expected = "\"timestamp_tz\"";
    Assertions.assertEquals(objectMapper.readTree(expected), objectMapper.readTree(jsonValue));

    type = Types.FixedType.of(10);
    jsonValue = JsonUtils.objectMapper().writeValueAsString(type);
    expected = "\"fixed(10)\"";
    Assertions.assertEquals(objectMapper.readTree(expected), objectMapper.readTree(jsonValue));

    type = Types.FixedCharType.of(20);
    jsonValue = JsonUtils.objectMapper().writeValueAsString(type);
    expected = "\"char(20)\"";
    Assertions.assertEquals(objectMapper.readTree(expected), objectMapper.readTree(jsonValue));

    type = Types.VarCharType.of(30);
    jsonValue = JsonUtils.objectMapper().writeValueAsString(type);
    expected = "\"varchar(30)\"";
    Assertions.assertEquals(objectMapper.readTree(expected), objectMapper.readTree(jsonValue));

    type = Types.DecimalType.of(10, 2);
    jsonValue = JsonUtils.objectMapper().writeValueAsString(type);
    expected = "\"decimal(10,2)\"";
    Assertions.assertEquals(objectMapper.readTree(expected), objectMapper.readTree(jsonValue));

    type =
        Types.StructType.of(
            Types.StructType.Field.nullableField("name", Types.StringType.get(), "name field"),
            Types.StructType.Field.notNullField("id", Types.IntegerType.get()));
    jsonValue = JsonUtils.objectMapper().writeValueAsString(type);
    expected =
        "{\n"
            + "    \"type\": \"struct\",\n"
            + "    \"fields\": [\n"
            + "        {\n"
            + "            \"name\": \"name\",\n"
            + "            \"type\": \"string\",\n"
            + "            \"nullable\": true,\n"
            + "            \"comment\": \"name field\"\n"
            + "        },\n"
            + "        {\n"
            + "            \"name\": \"id\",\n"
            + "            \"type\": \"integer\",\n"
            + "            \"nullable\": false\n"
            + "        }\n"
            + "    ]\n"
            + "}";
    Assertions.assertEquals(objectMapper.readTree(expected), objectMapper.readTree(jsonValue));

    type = Types.ListType.notNull(Types.FloatType.get());
    jsonValue = JsonUtils.objectMapper().writeValueAsString(type);
    expected =
        "{\n"
            + "    \"type\": \"list\",\n"
            + "    \"containsNull\": false,\n"
            + "    \"elementType\": \"float\"\n"
            + "}";
    Assertions.assertEquals(objectMapper.readTree(expected), objectMapper.readTree(jsonValue));

    type = Types.MapType.valueNullable(Types.ShortType.get(), Types.DateType.get());
    jsonValue = JsonUtils.objectMapper().writeValueAsString(type);
    expected =
        "{\n"
            + "    \"type\": \"map\",\n"
            + "    \"keyType\": \"short\",\n"
            + "    \"valueType\": \"date\",\n"
            + "    \"valueContainsNull\": true\n"
            + "}";
    Assertions.assertEquals(objectMapper.readTree(expected), objectMapper.readTree(jsonValue));

    type = Types.UnionType.of(Types.DecimalType.of(20, 8), Types.VarCharType.of(10));
    jsonValue = JsonUtils.objectMapper().writeValueAsString(type);
    expected =
        "{\n"
            + "    \"type\": \"union\",\n"
            + "    \"types\": [\n"
            + "        \"decimal(20,8)\",\n"
            + "        \"varchar(10)\"\n"
            + "    ]\n"
            + "}";
    Assertions.assertEquals(objectMapper.readTree(expected), objectMapper.readTree(jsonValue));

    type = Types.UnparsedType.of("user-defined");
    jsonValue = JsonUtils.objectMapper().writeValueAsString(type);
    expected =
        "{\n" + "    \"type\": \"unparsed\",\n" + "    \"unparsedType\": \"user-defined\"\n" + "}";
    Assertions.assertEquals(objectMapper.readTree(expected), objectMapper.readTree(jsonValue));
  }

  @Test
  void testGetLong() throws Exception {
    String jsonException = "{\"property\": \"value\"}";
    JsonNode nodeException = objectMapper.readTree(jsonException);

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> JsonUtils.getLong("property", nodeException));
    String jsonNormal = "{\"property\": 1}";
    JsonNode nodeNormal = objectMapper.readTree(jsonNormal);
    Long result = JsonUtils.getLong("property", nodeNormal);
    assertEquals(1L, result);
  }

  @Test
  void testPartitionDTOSerde() throws JsonProcessingException {
    String[] field1 = {"dt"};
    String[] field2 = {"country"};
    LiteralDTO literal1 =
        new LiteralDTO.Builder().withDataType(Types.DateType.get()).withValue("2008-08-08").build();
    LiteralDTO literal2 =
        new LiteralDTO.Builder().withDataType(Types.StringType.get()).withValue("us").build();
    PartitionDTO partition =
        IdentityPartitionDTO.builder()
            .withFieldNames(new String[][] {field1, field2})
            .withValues(new LiteralDTO[] {literal1, literal2})
            .build();
    String jsonValue = JsonUtils.objectMapper().writeValueAsString(partition);

    String expected =
        "{\n"
            + "  \"type\": \"identity\",\n"
            + "  \"fieldNames\": [\n"
            + "    [\n"
            + "      \"dt\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"country\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"values\": [\n"
            + "    {\n"
            + "      \"type\": \"literal\",\n"
            + "      \"dataType\": \"date\",\n"
            + "      \"value\": \"2008-08-08\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"type\": \"literal\",\n"
            + "      \"dataType\": \"string\",\n"
            + "      \"value\": \"us\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    Assertions.assertEquals(
        objectMapper.readValue(expected, IdentityPartitionDTO.class),
        objectMapper.readValue(jsonValue, IdentityPartitionDTO.class));

    partition =
        RangePartitionDTO.builder()
            .withName("p0")
            .withUpper(
                new LiteralDTO.Builder()
                    .withDataType(Types.NullType.get())
                    .withValue("null")
                    .build())
            .withLower(
                new LiteralDTO.Builder()
                    .withDataType(Types.IntegerType.get())
                    .withValue("6")
                    .build())
            .build();
    jsonValue = JsonUtils.objectMapper().writeValueAsString(partition);
    expected =
        "{\n"
            + "  \"type\": \"range\",\n"
            + "  \"name\": \"p0\",\n"
            + "  \"upper\": {\n"
            + "    \"type\": \"literal\",\n"
            + "    \"dataType\": \"null\",\n"
            + "    \"value\": \"null\"\n"
            + "  },\n"
            + "  \"lower\": {\n"
            + "    \"type\": \"literal\",\n"
            + "    \"dataType\": \"integer\",\n"
            + "    \"value\": \"6\"\n"
            + "  }\n"
            + "}";
    Assertions.assertEquals(
        objectMapper.readValue(expected, RangePartitionDTO.class),
        objectMapper.readValue(jsonValue, RangePartitionDTO.class));

    partition =
        ListPartitionDTO.builder()
            .withName("p202204_California")
            .withLists(
                new LiteralDTO[][] {
                  {
                    new LiteralDTO.Builder()
                        .withDataType(Types.DateType.get())
                        .withValue("2022-04-01")
                        .build(),
                    new LiteralDTO.Builder()
                        .withDataType(Types.StringType.get())
                        .withValue("Los Angeles")
                        .build()
                  },
                  {
                    new LiteralDTO.Builder()
                        .withDataType(Types.DateType.get())
                        .withValue("2022-04-01")
                        .build(),
                    new LiteralDTO.Builder()
                        .withDataType(Types.StringType.get())
                        .withValue("San Francisco")
                        .build()
                  }
                })
            .build();
    jsonValue = JsonUtils.objectMapper().writeValueAsString(partition);
    expected =
        "{\n"
            + "  \"type\": \"list\",\n"
            + "  \"name\": \"p202204_California\",\n"
            + "  \"lists\": [\n"
            + "    [\n"
            + "      {\n"
            + "        \"type\": \"literal\",\n"
            + "        \"dataType\": \"date\",\n"
            + "        \"value\": \"2022-04-01\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"type\": \"literal\",\n"
            + "        \"dataType\": \"string\",\n"
            + "        \"value\": \"Los Angeles\"\n"
            + "      }\n"
            + "    ],\n"
            + "    [\n"
            + "      {\n"
            + "        \"type\": \"literal\",\n"
            + "        \"dataType\": \"date\",\n"
            + "        \"value\": \"2022-04-01\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"type\": \"literal\",\n"
            + "        \"dataType\": \"string\",\n"
            + "        \"value\": \"San Francisco\"\n"
            + "      }\n"
            + "    ]\n"
            + "  ]\n"
            + "}";
    Assertions.assertEquals(
        objectMapper.readValue(expected, ListPartitionDTO.class),
        objectMapper.readValue(jsonValue, ListPartitionDTO.class));
  }

  @Test
  void testPartitionDTOSerdeException() {
    String illegalJson1 =
        "{\n"
            + "  \"type\": \"identity\",\n"
            + "  \"fieldNames\": [\n"
            + "    [\n"
            + "      \"dt\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"country\"\n"
            + "    ]\n"
            + "  ]\n"
            + "}";
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> objectMapper.readValue(illegalJson1, PartitionDTO.class));
    Assertions.assertTrue(
        exception.getMessage().contains("Identity partition must have array of values"),
        exception.getMessage());

    String illegalJson2 =
        "{\n"
            + "  \"type\": \"list\",\n"
            + "  \"lists\": [\n"
            + "    [\n"
            + "      {\n"
            + "        \"type\": \"literal\",\n"
            + "        \"dataType\": \"date\",\n"
            + "        \"value\": \"2022-04-01\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"type\": \"literal\",\n"
            + "        \"dataType\": \"string\",\n"
            + "        \"value\": \"Los Angeles\"\n"
            + "      }\n"
            + "    ],\n"
            + "    [\n"
            + "      {\n"
            + "        \"type\": \"literal\",\n"
            + "        \"dataType\": \"date\",\n"
            + "        \"value\": \"2022-04-01\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"type\": \"literal\",\n"
            + "        \"dataType\": \"string\",\n"
            + "        \"value\": \"San Francisco\"\n"
            + "      }\n"
            + "    ]\n"
            + "  ]\n"
            + "}";
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> objectMapper.readValue(illegalJson2, PartitionDTO.class));
    Assertions.assertTrue(
        exception.getMessage().contains("List partition must have name"), exception.getMessage());

    String illegalJson3 =
        "{\n"
            + "  \"type\": \"range\",\n"
            + "  \"upper\": {\n"
            + "    \"type\": \"literal\",\n"
            + "    \"dataType\": \"null\",\n"
            + "    \"value\": \"null\"\n"
            + "  },\n"
            + "  \"lower\": {\n"
            + "    \"type\": \"literal\",\n"
            + "    \"dataType\": \"integer\",\n"
            + "    \"value\": \"6\"\n"
            + "  }\n"
            + "}";
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> objectMapper.readValue(illegalJson3, PartitionDTO.class));
    Assertions.assertTrue(
        exception.getMessage().contains("Range partition must have name"), exception.getMessage());
  }

  @Test
  void testIndexDTOSerde() throws JsonProcessingException {
    Index idx1 =
        IndexDTO.builder()
            .withIndexType(Index.IndexType.UNIQUE_KEY)
            .withName("idx1")
            .withFieldNames(new String[][] {{"col_1"}})
            .build();

    String jsonValue = JsonUtils.objectMapper().writeValueAsString(idx1);

    String expected =
        "{\"indexType\":\"UNIQUE_KEY\",\"name\":\"idx1\",\"fieldNames\":[[\"col_1\"]]}";

    Assertions.assertEquals(
        objectMapper.readValue(expected, IndexDTO.class),
        objectMapper.readValue(jsonValue, IndexDTO.class));

    Index idx2 =
        IndexDTO.builder()
            .withIndexType(Index.IndexType.PRIMARY_KEY)
            .withName("idx2")
            .withFieldNames(new String[][] {{"col_2"}, {"col_3"}})
            .build();

    jsonValue = JsonUtils.objectMapper().writeValueAsString(idx2);

    expected =
        "{\"indexType\":\"PRIMARY_KEY\",\"name\":\"idx2\",\"fieldNames\":[[\"col_2\"],[\"col_3\"]]}";

    Assertions.assertEquals(
        objectMapper.readValue(expected, IndexDTO.class),
        objectMapper.readValue(jsonValue, IndexDTO.class));
  }
}
