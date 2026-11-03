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
package org.apache.gravitino.dto.requests;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.rest.RESTRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestNullRequestValidation {
  @ParameterizedTest
  @ValueSource(strings = {"{}", "{\"inUse\":null}"})
  void testMissingState(String json) throws Exception {
    for (Class<? extends RESTRequest> type :
        List.of(CatalogSetRequest.class, MetalakeSetRequest.class)) {
      RESTRequest request = JsonUtils.objectMapper().readValue(json, type);
      assertThrows(IllegalArgumentException.class, request::validate);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testExplicitState(boolean enabled) throws Exception {
    String json = "{\"inUse\":" + enabled + "}";
    CatalogSetRequest catalog = JsonUtils.objectMapper().readValue(json, CatalogSetRequest.class);
    MetalakeSetRequest metalake =
        JsonUtils.objectMapper().readValue(json, MetalakeSetRequest.class);
    catalog.validate();
    metalake.validate();
    assertEquals(enabled, catalog.isInUse());
    assertEquals(enabled, metalake.isInUse());
    assertEquals(enabled, new CatalogSetRequest(enabled).isInUse());
    assertEquals(enabled, new MetalakeSetRequest(enabled).isInUse());
  }

  @ParameterizedTest
  @ValueSource(strings = {"columns", "partitioning", "sortOrders", "indexes"})
  void testNullTableElement(String field) throws Exception {
    String json =
        "{\"name\":\"t\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\"}],\""
            + field
            + "\":[null]}";
    TableCreateRequest request = JsonUtils.objectMapper().readValue(json, TableCreateRequest.class);
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, request::validate);
    assertTrue(error.getMessage().contains(field));
  }

  @ParameterizedTest
  @ValueSource(strings = {"{\"name\":\"t\"}", "{\"name\":\"t\",\"columns\":null}"})
  void testMissingColumns(String json) throws Exception {
    TableCreateRequest request = JsonUtils.objectMapper().readValue(json, TableCreateRequest.class);
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, request::validate);
    assertEquals("\"columns\" field is required and cannot be null", error.getMessage());
  }

  @Test
  void testNullUpdates() throws Exception {
    for (Class<? extends RESTRequest> type :
        List.of(TableUpdatesRequest.class, SchemaUpdatesRequest.class, ViewUpdatesRequest.class)) {
      RESTRequest request = JsonUtils.objectMapper().readValue("{\"updates\":[null]}", type);
      assertThrows(IllegalArgumentException.class, request::validate);
    }
  }

  @Test
  void testNullPropertyValues() throws Exception {
    for (Class<? extends RESTRequest> type :
        List.of(
            MetalakeCreateRequest.class,
            CatalogCreateRequest.class,
            SchemaCreateRequest.class,
            FilesetCreateRequest.class,
            TopicCreateRequest.class,
            TableCreateRequest.class,
            ViewCreateRequest.class)) {
      String extra =
          type == CatalogCreateRequest.class
              ? ",\"type\":\"RELATIONAL\",\"provider\":\"hive\""
              : "";
      if (type == TableCreateRequest.class) {
        extra += ",\"columns\":[{\"name\":\"id\",\"type\":\"integer\"}]";
      }
      if (type == ViewCreateRequest.class) {
        extra +=
            ",\"representations\":[{\"type\":\"sql\",\"dialect\":\"trino\",\"sql\":\"SELECT 1\"}]";
      }
      String valid = "{\"name\":\"t\",\"properties\":null" + extra + "}";
      RESTRequest baseline = JsonUtils.objectMapper().readValue(valid, type);
      assertDoesNotThrow(baseline::validate);
      RESTRequest populated =
          JsonUtils.objectMapper()
              .readValue(
                  valid.replace("\"properties\":null", "\"properties\":{\"k\":\"v\"}"), type);
      assertDoesNotThrow(populated::validate);
      RESTRequest request =
          JsonUtils.objectMapper()
              .readValue(valid.replace("\"properties\":null", "\"properties\":{\"k\":null}"), type);
      IllegalArgumentException error =
          assertThrows(IllegalArgumentException.class, request::validate);
      assertTrue(error.getMessage().contains("properties"));
    }
  }

  @Test
  void testNullPropertyKey() {
    Map<String, String> properties = new HashMap<>();
    properties.put(null, "value");
    SchemaCreateRequest request = new SchemaCreateRequest("schema", null, properties);
    assertThrows(IllegalArgumentException.class, request::validate);
  }

  @Test
  void testNullableDefaultsAndOptionalFields() throws Exception {
    ColumnDTO unset =
        JsonUtils.objectMapper()
            .readValue(
                "{\"name\":\"id\",\"type\":\"integer\",\"defaultValue\":null}", ColumnDTO.class);
    assertSame(Column.DEFAULT_VALUE_NOT_SET, unset.defaultValue());
    ColumnDTO sqlNull =
        ColumnDTO.builder()
            .withName("id")
            .withDataType(Types.IntegerType.get())
            .withDefaultValue(Literals.NULL)
            .build();
    assertSame(Literals.NULL, sqlNull.defaultValue());
    TableCreateRequest request =
        JsonUtils.objectMapper()
            .readValue(
                "{\"name\":\"t\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\"}],\"properties\":null,\"partitioning\":null,\"sortOrders\":null,\"indexes\":null}",
                TableCreateRequest.class);
    assertDoesNotThrow(request::validate);
  }
}
