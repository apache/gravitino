/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Exercises the string-to-read-restrictions demo. */
public class TestReadRestrictionExpressionDemo {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(14, "region", Types.StringType.get()),
          Types.NestedField.optional(15, "score", Types.IntegerType.get()));

  @Test
  void testNameReferenceBecomesFieldId() throws Exception {
    String expressionJson = "{\"type\":\"eq\",\"term\":\"region\",\"value\":\"US\"}";

    JsonNode result =
        MAPPER.readTree(ReadRestrictionExpressionDemo.toReadRestrictions(expressionJson, SCHEMA));
    JsonNode filter = result.path("read-restrictions").path("required-row-filter");

    assertEquals("eq", filter.path("type").asText());
    assertEquals(14, filter.path("left").path("id").asInt());
    assertFalse(filter.path("left").has("name"));
    assertEquals("US", filter.path("right").path("value").asText());
  }

  @Test
  void testUnknownColumnFails() {
    String expressionJson = "{\"type\":\"eq\",\"term\":\"missing\",\"value\":\"US\"}";

    assertThrows(
        ValidationException.class,
        () -> ReadRestrictionExpressionDemo.toReadRestrictions(expressionJson, SCHEMA));
  }

  @Test
  void testUnsupportedPredicateFails() {
    String expressionJson = "{\"type\":\"not-eq\",\"term\":\"region\",\"value\":\"US\"}";

    assertThrows(
        IllegalArgumentException.class,
        () -> ReadRestrictionExpressionDemo.toReadRestrictions(expressionJson, SCHEMA));
  }

  @Test
  void testNonStringColumnFails() {
    String expressionJson = "{\"type\":\"eq\",\"term\":\"score\",\"value\":42}";

    assertThrows(
        IllegalArgumentException.class,
        () -> ReadRestrictionExpressionDemo.toReadRestrictions(expressionJson, SCHEMA));
  }
}
