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
package org.apache.gravitino.dto.semantic;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.semantic.DataType;
import org.junit.jupiter.api.Test;

public class TestSemanticDTOUtils {

  @Test
  public void testConvertArray() {
    assertNull(
        SemanticDTOUtils.convertArray(null, (Integer value) -> value.toString(), String[]::new));
    assertArrayEquals(
        new String[0],
        SemanticDTOUtils.convertArray(
            new Integer[0], (Integer value) -> value.toString(), String[]::new));
    assertArrayEquals(
        new String[] {"1", null, "3"},
        SemanticDTOUtils.convertArray(new Integer[] {1, null, 3}, Object::toString, String[]::new));
  }

  @Test
  public void testReadJsonValue() throws IOException {
    String json =
        "{\"text\":\"value\","
            + "\"trueValue\":true,"
            + "\"falseValue\":false,"
            + "\"integer\":123456789012345678901234567890,"
            + "\"decimal\":0.123456789012345678901234567890,"
            + "\"nullValue\":null,"
            + "\"array\":[1,\"two\",false,null],"
            + "\"object\":{\"first\":1,\"second\":2}}";

    try (JsonParser parser = new ObjectMapper().getFactory().createParser(json)) {
      assertEquals(JsonToken.START_OBJECT, parser.nextToken());
      Object value = SemanticDTOUtils.readJsonValue(parser, parser.currentToken());

      assertTrue(value instanceof Map);
      Map<?, ?> values = (Map<?, ?>) value;
      assertEquals(
          List.of(
              "text",
              "trueValue",
              "falseValue",
              "integer",
              "decimal",
              "nullValue",
              "array",
              "object"),
          new ArrayList<>(values.keySet()));
      assertEquals("value", values.get("text"));
      assertEquals(Boolean.TRUE, values.get("trueValue"));
      assertEquals(Boolean.FALSE, values.get("falseValue"));
      assertEquals(new BigInteger("123456789012345678901234567890"), values.get("integer"));
      assertEquals(new BigDecimal("0.123456789012345678901234567890"), values.get("decimal"));
      assertNull(values.get("nullValue"));

      List<?> array = (List<?>) values.get("array");
      assertEquals(4, array.size());
      assertEquals(BigInteger.ONE, array.get(0));
      assertEquals("two", array.get(1));
      assertEquals(Boolean.FALSE, array.get(2));
      assertNull(array.get(3));

      Map<?, ?> object = (Map<?, ?>) values.get("object");
      assertEquals(List.of("first", "second"), new ArrayList<>(object.keySet()));
      assertEquals(BigInteger.ONE, object.get("first"));
      assertEquals(BigInteger.TWO, object.get("second"));
    }
  }

  @Test
  public void testReadJsonValueRejectsUnsupportedToken() throws IOException {
    try (JsonParser parser = new ObjectMapper().getFactory().createParser("{\"value\":1}")) {
      assertEquals(JsonToken.START_OBJECT, parser.nextToken());
      assertEquals(JsonToken.FIELD_NAME, parser.nextToken());

      JsonMappingException exception =
          assertThrows(
              JsonMappingException.class,
              () -> SemanticDTOUtils.readJsonValue(parser, parser.currentToken()));
      assertEquals("Unsupported JSON token: FIELD_NAME", exception.getOriginalMessage());
    }
  }

  @Test
  public void testDataTypeSerializationAndDeserialization() throws IOException {
    ObjectMapper mapper = dataTypeMapper();
    for (Map.Entry<DataType, String> entry : dataTypeNames().entrySet()) {
      assertEquals("\"" + entry.getValue() + "\"", mapper.writeValueAsString(entry.getKey()));
      assertEquals(
          entry.getKey(), mapper.readValue("\"" + entry.getValue() + "\"", DataType.class));
    }
  }

  @Test
  public void testDataTypeDeserializationRejectsInvalidValues() {
    ObjectMapper mapper = dataTypeMapper();

    JsonMappingException unknownValue =
        assertThrows(
            JsonMappingException.class, () -> mapper.readValue("\"string\"", DataType.class));
    assertEquals(
        "Unknown Semantic Model data type: string. Supported values: "
            + String.join(", ", dataTypeNames().values()),
        unknownValue.getOriginalMessage());

    JsonMappingException nonString =
        assertThrows(JsonMappingException.class, () -> mapper.readValue("42", DataType.class));
    assertEquals(
        "DataType must be encoded as a string, but found VALUE_NUMBER_INT",
        nonString.getOriginalMessage());
  }

  private static ObjectMapper dataTypeMapper() {
    SimpleModule module = new SimpleModule();
    module.addSerializer(DataType.class, new SemanticDTOUtils.DataTypeSerializer());
    module.addDeserializer(DataType.class, new SemanticDTOUtils.DataTypeDeserializer());
    return new ObjectMapper().registerModule(module);
  }

  private static Map<DataType, String> dataTypeNames() {
    Map<DataType, String> names = new LinkedHashMap<>();
    names.put(DataType.STRING, "String");
    names.put(DataType.INTEGER, "Integer");
    names.put(DataType.DECIMAL, "Decimal");
    names.put(DataType.FLOAT, "Float");
    names.put(DataType.BOOLEAN, "Boolean");
    names.put(DataType.DATE, "Date");
    names.put(DataType.TIME, "Time");
    names.put(DataType.DATE_TIME, "DateTime");
    names.put(DataType.DATE_TIME_TZ, "DateTimeTz");
    names.put(DataType.OPAQUE, "Opaque");
    return names;
  }
}
