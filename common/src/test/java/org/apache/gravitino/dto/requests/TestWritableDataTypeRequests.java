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

import java.util.stream.Stream;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.rest.RESTRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestWritableDataTypeRequests {

  @ParameterizedTest
  @MethodSource("invalidColumnTypes")
  public void testRejectNewColumnTypes(String type, String path) throws Exception {
    TableCreateRequest request =
        JsonUtils.objectMapper()
            .readValue(
                "{\"name\":\"t\",\"columns\":[{\"name\":\"c\",\"type\":" + type + "}]}",
                TableCreateRequest.class);
    assertRejected(request, "columns[0].type" + path);

    TableUpdatesRequest add =
        JsonUtils.objectMapper()
            .readValue(
                "{\"updates\":[{\"@type\":\"addColumn\",\"fieldName\":[\"c\"],\"type\":"
                    + type
                    + "}]}",
                TableUpdatesRequest.class);
    assertRejected(add, "type" + path);

    TableUpdatesRequest update =
        JsonUtils.objectMapper()
            .readValue(
                "{\"updates\":[{\"@type\":\"updateColumnType\",\"fieldName\":[\"c\"],\"newType\":"
                    + type
                    + "}]}",
                TableUpdatesRequest.class);
    assertRejected(update, "newType" + path);
  }

  @Test
  public void testAcceptExplicitExternalAndNativeTypes() throws Exception {
    for (String type :
        new String[] {
          "\"integer\"",
          "{\"type\":\"external\",\"catalogString\":\"jsonb\"}",
          "{\"type\":\"list\",\"elementType\":{\"type\":\"external\",\"catalogString\":\"jsonb\"}}"
        }) {
      TableCreateRequest create =
          JsonUtils.objectMapper()
              .readValue(
                  "{\"name\":\"t\",\"columns\":[{\"name\":\"c\",\"type\":" + type + "}]}",
                  TableCreateRequest.class);
      Assertions.assertDoesNotThrow(create::validate);
      TableUpdatesRequest update =
          JsonUtils.objectMapper()
              .readValue(
                  "{\"updates\":[{\"@type\":\"updateColumnType\",\"fieldName\":[\"c\"],\"newType\":"
                      + type
                      + "}]}",
                  TableUpdatesRequest.class);
      Assertions.assertDoesNotThrow(update::validate);
    }
  }

  @ParameterizedTest
  @MethodSource("expressionRequests")
  public void testValidateLiteralTypesBeforeConversion(
      Class<? extends RESTRequest> requestClass, String json, String path) throws Exception {
    // A null literal value must not erase an unresolved type before validation.
    for (String value : new String[] {"null", "\"value\""}) {
      String literal =
          "{\"type\":\"literal\",\"dataType\":\"future_type\",\"value\":" + value + "}";
      RESTRequest invalid =
          JsonUtils.objectMapper().readValue(json.formatted(literal), requestClass);
      assertRejected(invalid, path);

      String blankExternal =
          literal.replace("\"future_type\"", "{\"type\":\"external\",\"catalogString\":\" \"}");
      assertRejected(
          JsonUtils.objectMapper().readValue(json.formatted(blankExternal), requestClass),
          path + ".catalogString");

      String external =
          literal.replace("\"future_type\"", "{\"type\":\"external\",\"catalogString\":\"jsonb\"}");
      RESTRequest valid =
          JsonUtils.objectMapper().readValue(json.formatted(external), requestClass);
      Assertions.assertDoesNotThrow(valid::validate);
    }
  }

  @Test
  public void testPreserveNullOptionsAndSqlDefaults() throws Exception {
    String json =
        """
        {"name":"events","columns":[
          {"name":"id","type":"integer","defaultValue":{
            "type":"literal","dataType":"integer","value":null}},
          {"name":"created_at","type":"timestamp","defaultValue":{
            "type":"unparsed","unparsedExpression":"CURRENT_TIMESTAMP"}}],
         "distribution":%s}
        """;
    for (String distribution : new String[] {"null", "{\"number\":0,\"funcArgs\":null}"}) {
      TableCreateRequest request =
          JsonUtils.objectMapper()
              .readValue(json.formatted(distribution), TableCreateRequest.class);
      Assertions.assertDoesNotThrow(request::validate);
    }
  }

  @Test
  public void testReadingLegacyColumnRetainsUnparsedType() throws Exception {
    ColumnDTO column =
        JsonUtils.objectMapper()
            .readValue(
                "{\"name\":\"legacy\",\"type\":\"future_type\",\"defaultValue\":{\"type\":\"literal\",\"dataType\":\"future_type\",\"value\":null}}",
                ColumnDTO.class);
    Assertions.assertInstanceOf(Types.UnparsedType.class, column.dataType());
    Assertions.assertDoesNotThrow(column::validate);
    ColumnDTO reloaded =
        JsonUtils.objectMapper()
            .readValue(JsonUtils.objectMapper().writeValueAsString(column), ColumnDTO.class);
    Assertions.assertEquals(column, reloaded);
  }

  private static Stream<Arguments> invalidColumnTypes() {
    return Stream.of(
        Arguments.of("\"future_type\"", ""),
        Arguments.of("{\"type\":\"unparsed\",\"unparsedType\":\"legacy\"}", ""),
        Arguments.of("{\"type\":\"external\",\"catalogString\":\" \"}", ".catalogString"),
        Arguments.of("{\"type\":\"list\",\"elementType\":\"future_type\"}", ".elementType"),
        Arguments.of(
            "{\"type\":\"map\",\"keyType\":\"future_type\",\"valueType\":\"integer\"}", ".keyType"),
        Arguments.of(
            "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"future_type\"}",
            ".valueType"),
        Arguments.of("{\"type\":\"union\",\"types\":[\"integer\",\"future_type\"]}", ".types[1]"),
        Arguments.of(
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"x\",\"type\":\"future_type\"}]}",
            ".fields[0].type"));
  }

  private static Stream<Arguments> expressionRequests() {
    return Stream.of(
        Arguments.of(
            TableCreateRequest.class,
            "{\"name\":\"t\",\"columns\":[{\"name\":\"c\",\"type\":\"string\",\"defaultValue\":%s}]}",
            "columns[0].defaultValue.dataType"),
        Arguments.of(
            TableUpdatesRequest.class,
            "{\"updates\":[{\"@type\":\"addColumn\",\"fieldName\":[\"c\"],\"type\":\"string\",\"defaultValue\":%s}]}",
            "defaultValue.dataType"),
        Arguments.of(
            TableUpdatesRequest.class,
            "{\"updates\":[{\"@type\":\"updateColumnDefaultValue\",\"fieldName\":[\"c\"],\"newDefaultValue\":{\"type\":\"function\",\"funcName\":\"f\",\"funcArgs\":[%s]}}]}",
            "newDefaultValue.funcArgs[0].dataType"),
        Arguments.of(
            FunctionRegisterRequest.class,
            "{\"name\":\"f\",\"functionType\":\"SCALAR\",\"deterministic\":true,\"definitions\":[{\"parameters\":[{\"name\":\"x\",\"dataType\":\"string\",\"defaultValue\":%s}],\"returnType\":\"string\"}]}",
            "definitions[0].parameters[0].defaultValue.dataType"),
        Arguments.of(
            FunctionUpdatesRequest.class,
            "{\"updates\":[{\"@type\":\"addDefinition\",\"definition\":{\"parameters\":[{\"name\":\"x\",\"dataType\":\"string\",\"defaultValue\":%s}],\"returnType\":\"string\"}}]}",
            "updates[0].definition.parameters[0].defaultValue.dataType"),
        Arguments.of(
            AddPartitionsRequest.class,
            "{\"partitions\":[{\"type\":\"identity\",\"name\":\"p\",\"fieldNames\":[[\"c\"]],\"values\":[%s]}]}",
            "partitions[0].values[0].dataType"),
        Arguments.of(
            AddPartitionsRequest.class,
            "{\"partitions\":[{\"type\":\"list\",\"name\":\"p\",\"lists\":[[%s]]}]}",
            "partitions[0].lists[0][0].dataType"),
        Arguments.of(
            AddPartitionsRequest.class,
            "{\"partitions\":[{\"type\":\"range\",\"name\":\"p\",\"lower\":%s,\"upper\":{\"type\":\"literal\",\"dataType\":\"integer\",\"value\":\"10\"}}]}",
            "partitions[0].lower.dataType"),
        Arguments.of(
            AddPartitionsRequest.class,
            "{\"partitions\":[{\"type\":\"range\",\"name\":\"p\",\"upper\":%s,\"lower\":{\"type\":\"literal\",\"dataType\":\"integer\",\"value\":\"0\"}}]}",
            "partitions[0].upper.dataType"),
        Arguments.of(
            TableCreateRequest.class,
            "{\"name\":\"t\",\"columns\":[{\"name\":\"c\",\"type\":\"string\"}],\"partitioning\":[{\"strategy\":\"list\",\"fieldNames\":[[\"c\"]],\"assignments\":[{\"type\":\"list\",\"name\":\"p\",\"lists\":[[%s]]}]}]}",
            "partitioning[0].assignments[0].lists[0][0].dataType"),
        Arguments.of(
            TableCreateRequest.class,
            "{\"name\":\"t\",\"columns\":[{\"name\":\"c\",\"type\":\"string\"}],\"partitioning\":[{\"strategy\":\"function\",\"funcName\":\"f\",\"funcArgs\":[%s]}]}",
            "partitioning[0].funcArgs[0].dataType"),
        Arguments.of(
            TableCreateRequest.class,
            "{\"name\":\"t\",\"columns\":[{\"name\":\"c\",\"type\":\"string\"}],\"distribution\":{\"strategy\":\"hash\",\"number\":2,\"funcArgs\":[%s]}}",
            "distribution.funcArgs[0].dataType"),
        Arguments.of(
            TableCreateRequest.class,
            "{\"name\":\"t\",\"columns\":[{\"name\":\"c\",\"type\":\"string\"}],\"sortOrders\":[{\"sortTerm\":%s,\"direction\":\"asc\",\"nullOrdering\":\"nulls_first\"}]}",
            "sortOrders[0].sortTerm.dataType"));
  }

  private static void assertRejected(RESTRequest request, String path) {
    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, request::validate);
    Assertions.assertTrue(exception.getMessage().contains(path), exception.getMessage());
  }
}
