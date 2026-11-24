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
    assertRejected(add, "updates[0].type" + path);

    TableUpdatesRequest update =
        JsonUtils.objectMapper()
            .readValue(
                "{\"updates\":[{\"@type\":\"updateColumnType\",\"fieldName\":[\"c\"],\"newType\":"
                    + type
                    + "}]}",
                TableUpdatesRequest.class);
    assertRejected(update, "updates[0].newType" + path);
  }

  @ParameterizedTest
  @MethodSource("invalidTableUpdates")
  public void testTableUpdateErrorPathsIncludeIndex(String update, String path) throws Exception {
    TableUpdatesRequest request =
        JsonUtils.objectMapper()
            .readValue(
                """
                {"updates":[{"@type":"updateComment","newComment":"updated"},%s]}
                """
                    .formatted(update),
                TableUpdatesRequest.class);
    assertRejected(request, "updates[1]." + path);
    assertRejected(request.getUpdates().get(1), path);
  }

  @Test
  public void testPreserveValidationForUpdatesWithoutTypes() throws Exception {
    String json = "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"\"}]}";
    for (RESTRequest request :
        new RESTRequest[] {
          JsonUtils.objectMapper().readValue(json, TableUpdatesRequest.class),
          JsonUtils.objectMapper().readValue(json, ViewUpdatesRequest.class)
        }) {
      IllegalArgumentException exception =
          Assertions.assertThrows(IllegalArgumentException.class, request::validate);
      Assertions.assertEquals(
          "\"newName\" field is required and cannot be empty", exception.getMessage());
    }
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
  @MethodSource("invalidColumnTypes")
  public void testRejectNewViewColumnTypes(String type, String path) throws Exception {
    ViewCreateRequest create =
        JsonUtils.objectMapper()
            .readValue(
                """
                {"name":"v","columns":[{"name":"c","type":%s}],
                 "representations":[{"type":"sql","dialect":"trino","sql":"SELECT 1"}]}
                """
                    .formatted(type),
                ViewCreateRequest.class);
    assertRejected(create, "columns[0].type" + path);

    ViewUpdatesRequest replace =
        JsonUtils.objectMapper()
            .readValue(
                """
                {"updates":[{"@type":"setProperty","property":"k","value":"v"},
                  {"@type":"replaceView","columns":[{"name":"c","type":%s}],
                   "representations":[{"type":"sql","dialect":"trino","sql":"SELECT 1"}]}]}
                """
                    .formatted(type),
                ViewUpdatesRequest.class);
    assertRejected(replace, "updates[1].columns[0].type" + path);
    assertRejected(replace.getUpdates().get(1), "columns[0].type" + path);
  }

  @Test
  public void testAcceptOptionalAndWritableViewColumns() throws Exception {
    for (String columns :
        new String[] {
          "null",
          "[]",
          """
          [{"name":"c","type":"timestamp","defaultValue":{
            "type":"unparsed","unparsedExpression":"CURRENT_TIMESTAMP"}}]
          """,
          """
          [{"name":"c","type":{"type":"external","catalogString":"jsonb"}}]
          """,
          """
          [{"name":"c","type":{"type":"list","elementType":{
            "type":"external","catalogString":"jsonb"}}}]
          """
        }) {
      ViewCreateRequest create =
          JsonUtils.objectMapper()
              .readValue(
                  """
                  {"name":"v","columns":%s,
                   "representations":[{"type":"sql","dialect":"trino","sql":"SELECT 1"}]}
                  """
                      .formatted(columns),
                  ViewCreateRequest.class);
      Assertions.assertDoesNotThrow(create::validate);
      ViewUpdatesRequest replace =
          JsonUtils.objectMapper()
              .readValue(
                  """
                  {"updates":[{"@type":"replaceView","columns":%s,
                    "representations":[{"type":"sql","dialect":"trino","sql":"SELECT 1"}]}]}
                  """
                      .formatted(columns),
                  ViewUpdatesRequest.class);
      Assertions.assertDoesNotThrow(replace::validate);
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
            ViewCreateRequest.class,
            """
            {"name":"v","columns":[{"name":"c","type":"string","defaultValue":%s}],
             "representations":[{"type":"sql","dialect":"trino","sql":"SELECT 1"}]}
            """,
            "columns[0].defaultValue.dataType"),
        Arguments.of(
            ViewUpdatesRequest.class,
            """
            {"updates":[{"@type":"replaceView","columns":[
              {"name":"c","type":"string","defaultValue":%s}],
              "representations":[{"type":"sql","dialect":"trino","sql":"SELECT 1"}]}]}
            """,
            "updates[0].columns[0].defaultValue.dataType"),
        Arguments.of(
            TableUpdatesRequest.class,
            "{\"updates\":[{\"@type\":\"addColumn\",\"fieldName\":[\"c\"],\"type\":\"string\",\"defaultValue\":%s}]}",
            "updates[0].defaultValue.dataType"),
        Arguments.of(
            TableUpdatesRequest.class,
            "{\"updates\":[{\"@type\":\"updateColumnDefaultValue\",\"fieldName\":[\"c\"],\"newDefaultValue\":{\"type\":\"function\",\"funcName\":\"f\",\"funcArgs\":[%s]}}]}",
            "updates[0].newDefaultValue.funcArgs[0].dataType"),
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

  private static Stream<Arguments> invalidTableUpdates() {
    return Stream.of(
        Arguments.of(
            """
            {"@type":"addColumn","fieldName":["c"],
             "type":{"type":"list","elementType":"future_type"}}
            """,
            "type.elementType"),
        Arguments.of(
            """
            {"@type":"addColumn","fieldName":["c"],"type":"string",
             "defaultValue":{"type":"function","funcName":"f","funcArgs":[
               {"type":"literal","dataType":"future_type","value":null}]}}
            """,
            "defaultValue.funcArgs[0].dataType"),
        Arguments.of(
            """
            {"@type":"updateColumnType","fieldName":["c"],
             "newType":{"type":"struct","fields":[{"name":"nested","type":"future_type"}]}}
            """,
            "newType.fields[0].type"),
        Arguments.of(
            """
            {"@type":"updateColumnDefaultValue","fieldName":["c"],
             "newDefaultValue":{"type":"function","funcName":"f","funcArgs":[
               {"type":"literal","dataType":{"type":"external","catalogString":" "},"value":null}]}}
            """,
            "newDefaultValue.funcArgs[0].dataType.catalogString"));
  }

  private static void assertRejected(RESTRequest request, String path) {
    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, request::validate);
    Assertions.assertTrue(exception.getMessage().contains(path), exception.getMessage());
  }
}
