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

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.gravitino.dto.rel.expressions.LiteralDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableUpdatesRequest {
  @Test
  public void testTableUpdatesRequest() throws JsonProcessingException {
    List<TableUpdateRequest> updates =
        ImmutableList.of(
            new TableUpdateRequest.RenameTableRequest("newTable"),
            new TableUpdateRequest.UpdateTableCommentRequest("comment"),
            new TableUpdateRequest.SetTablePropertyRequest("key", "value"),
            new TableUpdateRequest.RemoveTablePropertyRequest("key"),
            new TableUpdateRequest.RenameTableColumnRequest(
                new String[] {"oldColumn"}, "newColumn"),
            new TableUpdateRequest.UpdateTableColumnDefaultValueRequest(
                new String[] {"column"},
                LiteralDTO.builder()
                    .withDataType(Types.DateType.get())
                    .withValue("2023-04-01")
                    .build()),
            new TableUpdateRequest.UpdateTableColumnTypeRequest(
                new String[] {"column"}, Types.StringType.get()),
            new TableUpdateRequest.UpdateTableColumnCommentRequest(
                new String[] {"column"}, "comment"),
            new TableUpdateRequest.UpdateTableColumnPositionRequest(
                new String[] {"column"}, TableChange.ColumnPosition.first()),
            new TableUpdateRequest.UpdateTableColumnNullabilityRequest(
                new String[] {"column"}, false),
            new TableUpdateRequest.DeleteTableColumnRequest(new String[] {"column"}, true));

    TableUpdatesRequest tableUpdatesRequest = new TableUpdatesRequest(updates);
    String jsonString = JsonUtils.objectMapper().writeValueAsString(tableUpdatesRequest);
    String expected =
        "{\n"
            + "  \"updates\": [\n"
            + "    {\n"
            + "      \"@type\": \"rename\",\n"
            + "      \"newName\": \"newTable\",\n"
            + "      \"newSchemaName\": null\n"
            + "    },\n"
            + "    {\n"
            + "      \"@type\": \"updateComment\",\n"
            + "      \"newComment\": \"comment\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"@type\": \"setProperty\",\n"
            + "      \"property\": \"key\",\n"
            + "      \"value\": \"value\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"@type\": \"removeProperty\",\n"
            + "      \"property\": \"key\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"@type\": \"renameColumn\",\n"
            + "      \"oldFieldName\": [\n"
            + "        \"oldColumn\"\n"
            + "      ],\n"
            + "      \"newFieldName\": \"newColumn\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"@type\": \"updateColumnDefaultValue\",\n"
            + "      \"fieldName\": [\n"
            + "        \"column\"\n"
            + "      ],\n"
            + "      \"newDefaultValue\": {\n"
            + "        \"type\": \"literal\",\n"
            + "        \"dataType\": \"date\",\n"
            + "        \"value\": \"2023-04-01\"\n"
            + "      }\n"
            + "    },\n"
            + "    {\n"
            + "      \"@type\": \"updateColumnType\",\n"
            + "      \"fieldName\": [\n"
            + "        \"column\"\n"
            + "      ],\n"
            + "      \"newType\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"@type\": \"updateColumnComment\",\n"
            + "      \"fieldName\": [\n"
            + "        \"column\"\n"
            + "      ],\n"
            + "      \"newComment\": \"comment\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"@type\": \"updateColumnPosition\",\n"
            + "      \"fieldName\": [\n"
            + "        \"column\"\n"
            + "      ],\n"
            + "      \"newPosition\": \"first\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"@type\": \"updateColumnNullability\",\n"
            + "      \"fieldName\": [\n"
            + "        \"column\"\n"
            + "      ],\n"
            + "      \"nullable\": false\n"
            + "    },\n"
            + "    {\n"
            + "      \"@type\": \"deleteColumn\",\n"
            + "      \"fieldName\": [\n"
            + "        \"column\"\n"
            + "      ],\n"
            + "      \"ifExists\": true\n"
            + "    }\n"
            + "  ]\n"
            + "}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(jsonString));

    // validate backward compatibility for renameTable without newSchemaName
    String renameTableJsonString =
        "{\n"
            + "  \"updates\": [\n"
            + "    {\n"
            + "      \"@type\": \"rename\",\n"
            + "      \"newName\": \"newTable\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";
    TableUpdatesRequest renameTableRequest =
        JsonUtils.objectMapper().readValue(renameTableJsonString, TableUpdatesRequest.class);
    Assertions.assertEquals(1, renameTableRequest.getUpdates().size());
    TableUpdateRequest renameTable = renameTableRequest.getUpdates().get(0);
    Assertions.assertInstanceOf(TableUpdateRequest.RenameTableRequest.class, renameTable);
    Assertions.assertEquals(
        "newTable", ((TableUpdateRequest.RenameTableRequest) renameTable).getNewName());
    Assertions.assertNull(((TableUpdateRequest.RenameTableRequest) renameTable).getNewSchemaName());

    // test validate blank property value
    TableUpdateRequest.SetTablePropertyRequest setTablePropertyRequest =
        new TableUpdateRequest.SetTablePropertyRequest("key", " ");
    Assertions.assertDoesNotThrow(setTablePropertyRequest::validate);

    setTablePropertyRequest = new TableUpdateRequest.SetTablePropertyRequest("key", "");
    Assertions.assertDoesNotThrow(setTablePropertyRequest::validate);

    // test validate DEFAULT_VALUE_NOT_SET or null property value
    TableUpdateRequest.UpdateTableColumnDefaultValueRequest updateTableColumnDefaultValueRequest =
        new TableUpdateRequest.UpdateTableColumnDefaultValueRequest(
            new String[] {"key"}, DEFAULT_VALUE_NOT_SET);
    Throwable exception1 =
        Assertions.assertThrows(
            IllegalArgumentException.class, updateTableColumnDefaultValueRequest::validate);
    Assertions.assertTrue(
        exception1.getMessage().contains("field is required and cannot be empty"));

    updateTableColumnDefaultValueRequest =
        new TableUpdateRequest.UpdateTableColumnDefaultValueRequest(new String[] {"key"}, null);
    Throwable exception2 =
        Assertions.assertThrows(
            IllegalArgumentException.class, updateTableColumnDefaultValueRequest::validate);
    Assertions.assertTrue(
        exception2.getMessage().contains("field is required and cannot be empty"));
  }

  @Test
  public void testAddTableColumnRequest() throws JsonProcessingException {
    TableUpdateRequest addTableColumnRequest =
        new TableUpdateRequest.AddTableColumnRequest(
            new String[] {"column"},
            Types.StringType.get(),
            "comment",
            TableChange.ColumnPosition.after("afterColumn"),
            false,
            false,
            LiteralDTO.builder().withDataType(Types.StringType.get()).withValue("hello").build());
    String jsonString = JsonUtils.objectMapper().writeValueAsString(addTableColumnRequest);
    String expected =
        "{\n"
            + "  \"@type\": \"addColumn\",\n"
            + "  \"fieldName\": [\n"
            + "    \"column\"\n"
            + "  ],\n"
            + "  \"type\": \"string\",\n"
            + "  \"comment\": \"comment\",\n"
            + "  \"position\": {\n"
            + "    \"after\": \"afterColumn\"\n"
            + "  },\n"
            + "  \"nullable\": false,\n"
            + "  \"autoIncrement\": false,\n"
            + "  \"defaultValue\": {\n"
            + "    \"type\": \"literal\",\n"
            + "    \"dataType\": \"string\",\n"
            + "    \"value\": \"hello\"\n"
            + "  }\n"
            + "}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(jsonString));

    // test default nullability
    addTableColumnRequest =
        new TableUpdateRequest.AddTableColumnRequest(
            new String[] {"column"},
            Types.StringType.get(),
            "test default nullability",
            TableChange.ColumnPosition.first());
    jsonString = JsonUtils.objectMapper().writeValueAsString(addTableColumnRequest);
    expected =
        "{\n"
            + "  \"@type\": \"addColumn\",\n"
            + "  \"fieldName\": [\n"
            + "    \"column\"\n"
            + "  ],\n"
            + "  \"type\": \"string\",\n"
            + "  \"comment\": \"test default nullability\",\n"
            + "  \"position\": \"first\",\n"
            + "  \"nullable\": true,\n"
            + "  \"autoIncrement\": false\n"
            + "}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(jsonString));
    Assertions.assertTrue(
        JsonUtils.objectMapper()
                .readValue(
                    jsonString.replace("first", "FIRST"),
                    TableUpdateRequest.AddTableColumnRequest.class)
                .getPosition()
            instanceof TableChange.First);

    // test default position
    addTableColumnRequest =
        new TableUpdateRequest.AddTableColumnRequest(
            new String[] {"column"}, Types.StringType.get(), "test default position");
    jsonString = JsonUtils.objectMapper().writeValueAsString(addTableColumnRequest);
    expected =
        "{\n"
            + "  \"@type\": \"addColumn\",\n"
            + "  \"fieldName\": [\n"
            + "    \"column\"\n"
            + "  ],\n"
            + "  \"type\": \"string\",\n"
            + "  \"comment\": \"test default position\",\n"
            + "  \"position\": \"default\",\n"
            + "  \"nullable\": true,\n"
            + "  \"autoIncrement\": false\n"
            + "}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(jsonString));
    Assertions.assertTrue(
        JsonUtils.objectMapper()
                .readValue(
                    jsonString.replace("default", "DEFAULT"),
                    TableUpdateRequest.AddTableColumnRequest.class)
                .getPosition()
            instanceof TableChange.Default);
  }

  @Test
  public void testOperationTableIndexRequest() throws JsonProcessingException {
    // check add index request
    TableUpdateRequest tableUpdateRequest =
        new TableUpdateRequest.AddTableIndexRequest(
            Index.IndexType.PRIMARY_KEY,
            Indexes.DEFAULT_MYSQL_PRIMARY_KEY_NAME,
            new String[][] {{"column1"}});
    String jsonString = JsonUtils.objectMapper().writeValueAsString(tableUpdateRequest);
    String expected =
        "{\"@type\":\"addTableIndex\",\"index\":{\"indexType\":\"PRIMARY_KEY\",\"name\":\"PRIMARY\",\"fieldNames\":[[\"column1\"]]}}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(jsonString));

    tableUpdateRequest =
        new TableUpdateRequest.AddTableIndexRequest(
            Index.IndexType.UNIQUE_KEY, "uk_2", new String[][] {{"column2"}});
    jsonString = JsonUtils.objectMapper().writeValueAsString(tableUpdateRequest);
    expected =
        "{\"@type\":\"addTableIndex\",\"index\":{\"indexType\":\"UNIQUE_KEY\",\"name\":\"uk_2\",\"fieldNames\":[[\"column2\"]]}}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(jsonString));

    // check delete index request
    TableUpdateRequest.DeleteTableIndexRequest deleteTableIndexRequest =
        new TableUpdateRequest.DeleteTableIndexRequest("uk_2", true);
    jsonString = JsonUtils.objectMapper().writeValueAsString(deleteTableIndexRequest);
    expected = "{\"@type\":\"deleteTableIndex\",\"name\":\"uk_2\",\"ifExists\":true}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(jsonString));
  }
}
