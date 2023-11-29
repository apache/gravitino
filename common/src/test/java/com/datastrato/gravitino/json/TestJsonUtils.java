/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
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
  }
}
