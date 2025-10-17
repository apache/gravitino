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
import java.util.Arrays;
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

  @Test
  public void testValidateValidTableUpdateRequests() {
    // Test valid requests
    List<TableUpdateRequest> validRequests =
        Arrays.asList(
            new TableUpdateRequest.RenameTableRequest("newTable"),
            new TableUpdateRequest.UpdateTableCommentRequest("comment"),
            new TableUpdateRequest.SetTablePropertyRequest("property", "value"),
            new TableUpdateRequest.RemoveTablePropertyRequest("property"),
            new TableUpdateRequest.AddTableColumnRequest(
                new String[] {"column"}, Types.StringType.get(), "comment"),
            new TableUpdateRequest.RenameTableColumnRequest(
                new String[] {"oldColumn"}, "newColumn"),
            new TableUpdateRequest.UpdateTableColumnDefaultValueRequest(
                new String[] {"column"},
                LiteralDTO.builder()
                    .withDataType(Types.StringType.get())
                    .withValue("default")
                    .build()),
            new TableUpdateRequest.UpdateTableColumnTypeRequest(
                new String[] {"column"}, Types.StringType.get()),
            new TableUpdateRequest.UpdateTableColumnCommentRequest(
                new String[] {"column"}, "new comment"),
            new TableUpdateRequest.UpdateTableColumnPositionRequest(
                new String[] {"column"}, TableChange.ColumnPosition.first()),
            new TableUpdateRequest.UpdateTableColumnNullabilityRequest(
                new String[] {"column"}, false),
            new TableUpdateRequest.DeleteTableColumnRequest(new String[] {"column"}, false),
            new TableUpdateRequest.AddTableIndexRequest(
                Index.IndexType.PRIMARY_KEY, "index_name", new String[][] {{"column1"}}),
            new TableUpdateRequest.DeleteTableIndexRequest("index_name", true),
            new TableUpdateRequest.UpdateColumnAutoIncrementRequest(new String[] {"column"}, true));

    for (TableUpdateRequest request : validRequests) {
      Assertions.assertDoesNotThrow(request::validate);
    }
  }

  @Test
  public void testValidateInvalidTableUpdateRequests() {
    // Test invalid requests

    // RenameTableRequest - null newName
    IllegalArgumentException renameTableException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new TableUpdateRequest.RenameTableRequest(null).validate());
    Assertions.assertTrue(renameTableException.getMessage().contains("newName"));
    // RenameTableRequest - empty newName
    IllegalArgumentException renameTableException2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new TableUpdateRequest.RenameTableRequest("").validate());
    Assertions.assertTrue(renameTableException2.getMessage().contains("newName"));

    // SetTablePropertyRequest - null property
    IllegalArgumentException setTablePropertyException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new TableUpdateRequest.SetTablePropertyRequest(null, "value").validate());
    Assertions.assertTrue(setTablePropertyException.getMessage().contains("property"));

    // SetTablePropertyRequest - empty property
    IllegalArgumentException setTablePropertyException2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new TableUpdateRequest.SetTablePropertyRequest("", "value").validate());
    Assertions.assertTrue(setTablePropertyException2.getMessage().contains("property"));

    // SetTablePropertyRequest - null value
    IllegalArgumentException setTablePropertyException3 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new TableUpdateRequest.SetTablePropertyRequest("property", null).validate());
    Assertions.assertTrue(setTablePropertyException3.getMessage().contains("value"));

    // RemoveTablePropertyRequest - null property
    IllegalArgumentException removeTablePropertyException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new TableUpdateRequest.RemoveTablePropertyRequest(null).validate());
    Assertions.assertTrue(removeTablePropertyException.getMessage().contains("property"));

    // RemoveTablePropertyRequest - empty property
    IllegalArgumentException removeTablePropertyException2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new TableUpdateRequest.RemoveTablePropertyRequest("").validate());
    Assertions.assertTrue(removeTablePropertyException2.getMessage().contains("property"));

    // AddTableColumnRequest - null fieldName
    IllegalArgumentException addTableColException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.AddTableColumnRequest(
                        null, Types.StringType.get(), "comment")
                    .validate());
    Assertions.assertTrue(addTableColException.getMessage().contains("fieldName"));

    // AddTableColumnRequest - empty fieldName
    IllegalArgumentException addTableColException2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.AddTableColumnRequest(
                        new String[] {}, Types.StringType.get(), "comment")
                    .validate());
    Assertions.assertTrue(addTableColException2.getMessage().contains("fieldName"));

    // AddTableColumnRequest - fieldName with blank elements
    IllegalArgumentException addTableColException3 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.AddTableColumnRequest(
                        new String[] {"column", "", "column2"}, Types.StringType.get(), "comment")
                    .validate());
    Assertions.assertTrue(addTableColException3.getMessage().contains("fieldName"));

    // AddTableColumnRequest - fieldName with whitespace elements
    IllegalArgumentException addTableColException4 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.AddTableColumnRequest(
                        new String[] {"column", "   ", "column2"},
                        Types.StringType.get(),
                        "comment")
                    .validate());
    Assertions.assertTrue(addTableColException4.getMessage().contains("fieldName"));

    // AddTableColumnRequest - null dataType
    IllegalArgumentException addTableColumnDataTypeException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.AddTableColumnRequest(
                        new String[] {"column"}, null, "comment")
                    .validate());
    Assertions.assertTrue(addTableColumnDataTypeException.getMessage().contains("type"));

    // RenameTableColumnRequest - null oldFieldName
    IllegalArgumentException renameTableColException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new TableUpdateRequest.RenameTableColumnRequest(null, "newColumn").validate());
    Assertions.assertTrue(renameTableColException.getMessage().contains("oldFieldName"));

    // RenameTableColumnRequest - empty oldFieldName
    IllegalArgumentException renameTableColException2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.RenameTableColumnRequest(new String[] {}, "newColumn")
                    .validate());
    Assertions.assertTrue(renameTableColException2.getMessage().contains("oldFieldName"));

    // RenameTableColumnRequest - oldFieldName with blank elements
    IllegalArgumentException renameTableColException4 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.RenameTableColumnRequest(
                        new String[] {"oldColumn", "", "oldColumn2"}, "newColumn")
                    .validate());
    Assertions.assertTrue(renameTableColException4.getMessage().contains("oldFieldName"));

    // RenameTableColumnRequest - oldFieldName with whitespace elements
    IllegalArgumentException renameTableColException5 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.RenameTableColumnRequest(
                        new String[] {"oldColumn", "   ", "oldColumn2"}, "newColumn")
                    .validate());
    Assertions.assertTrue(renameTableColException5.getMessage().contains("oldFieldName"));

    // RenameTableColumnRequest - null newFieldName
    IllegalArgumentException renameTableColumnException3 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.RenameTableColumnRequest(new String[] {"oldColumn"}, null)
                    .validate());
    Assertions.assertTrue(renameTableColumnException3.getMessage().contains("newFieldName"));

    // UpdateTableColumnDefaultValueRequest - null newDefaultValue
    IllegalArgumentException updateColumnDefaultValueException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.UpdateTableColumnDefaultValueRequest(
                        new String[] {"column"}, null)
                    .validate());
    Assertions.assertTrue(
        updateColumnDefaultValueException.getMessage().contains("newDefaultValue"));

    // UpdateTableColumnDefaultValueRequest - empty fieldName
    IllegalArgumentException updateColumnDefaultValueException2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.UpdateTableColumnDefaultValueRequest(
                        new String[] {},
                        LiteralDTO.builder()
                            .withDataType(Types.StringType.get())
                            .withValue("default")
                            .build())
                    .validate());
    Assertions.assertTrue(updateColumnDefaultValueException2.getMessage().contains("fieldName"));

    // UpdateTableColumnDefaultValueRequest - fieldName with blank elements
    IllegalArgumentException updateColumnDefaultValueException3 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.UpdateTableColumnDefaultValueRequest(
                        new String[] {"column", "", "column2"},
                        LiteralDTO.builder()
                            .withDataType(Types.StringType.get())
                            .withValue("default")
                            .build())
                    .validate());
    Assertions.assertTrue(updateColumnDefaultValueException3.getMessage().contains("fieldName"));

    // UpdateTableColumnTypeRequest - null newType
    IllegalArgumentException updateColumnTypeException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.UpdateTableColumnTypeRequest(new String[] {"column"}, null)
                    .validate());
    Assertions.assertTrue(updateColumnTypeException.getMessage().contains("newType"));

    // UpdateTableColumnTypeRequest - empty fieldName
    IllegalArgumentException updateColumnTypeException2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.UpdateTableColumnTypeRequest(
                        new String[] {}, Types.StringType.get())
                    .validate());
    Assertions.assertTrue(updateColumnTypeException2.getMessage().contains("fieldName"));

    // UpdateTableColumnTypeRequest - fieldName with blank elements
    IllegalArgumentException updateColumnTypeException3 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.UpdateTableColumnTypeRequest(
                        new String[] {"column", "", "column2"}, Types.StringType.get())
                    .validate());
    Assertions.assertTrue(updateColumnTypeException3.getMessage().contains("fieldName"));

    // UpdateTableColumnCommentRequest - null newComment
    IllegalArgumentException updateColumnCommentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.UpdateTableColumnCommentRequest(
                        new String[] {"column"}, null)
                    .validate());
    Assertions.assertTrue(updateColumnCommentException.getMessage().contains("newComment"));

    // UpdateTableColumnCommentRequest - empty newComment
    IllegalArgumentException updateColumnCommentException2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.UpdateTableColumnCommentRequest(new String[] {"column"}, "")
                    .validate());
    Assertions.assertTrue(updateColumnCommentException2.getMessage().contains("newComment"));

    // UpdateTableColumnCommentRequest - empty fieldName
    IllegalArgumentException updateColumnCommentException3 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.UpdateTableColumnCommentRequest(new String[] {}, "comment")
                    .validate());
    Assertions.assertTrue(updateColumnCommentException3.getMessage().contains("fieldName"));

    // UpdateTableColumnPositionRequest - null newPosition
    IllegalArgumentException updateColumnPositionException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.UpdateTableColumnPositionRequest(
                        new String[] {"column"}, null)
                    .validate());
    Assertions.assertTrue(updateColumnPositionException.getMessage().contains("newPosition"));

    // UpdateTableColumnPositionRequest - empty fieldName
    IllegalArgumentException updateColumnPositionException2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.UpdateTableColumnPositionRequest(
                        new String[] {}, TableChange.ColumnPosition.first())
                    .validate());
    Assertions.assertTrue(updateColumnPositionException2.getMessage().contains("fieldName"));

    // DeleteTableIndexRequest - null name
    IllegalArgumentException deleteTableIndexException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new TableUpdateRequest.DeleteTableIndexRequest(null, true).validate());
    Assertions.assertTrue(deleteTableIndexException.getMessage().contains("name"));

    // DeleteTableIndexRequest - empty name
    IllegalArgumentException deleteTableIndexException2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new TableUpdateRequest.DeleteTableIndexRequest("", true).validate());
    Assertions.assertTrue(deleteTableIndexException2.getMessage().contains("name"));

    // AddTableIndexRequest - null index (using default constructor)
    IllegalArgumentException addTableIndexException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new TableUpdateRequest.AddTableIndexRequest().validate());
    Assertions.assertTrue(addTableIndexException.getMessage().contains("Index"));
    // AddTableIndexRequest - null index type
    IllegalArgumentException addTableIndexException2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.AddTableIndexRequest(
                        null, "index_name", new String[][] {{"column1"}})
                    .validate());
    Assertions.assertTrue(addTableIndexException2.getMessage().contains("type"));

    // AddTableIndexRequest - null field names
    IllegalArgumentException addTableIndexException3 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.AddTableIndexRequest(
                        Index.IndexType.PRIMARY_KEY, "index_name", null)
                    .validate());
    Assertions.assertTrue(addTableIndexException3.getMessage().contains("column names"));

    // AddTableIndexRequest - empty field names
    IllegalArgumentException addTableIndexException4 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.AddTableIndexRequest(
                        Index.IndexType.PRIMARY_KEY, "index_name", new String[][] {})
                    .validate());
    Assertions.assertTrue(addTableIndexException4.getMessage().contains("column names"));

    // AddTableIndexRequest - null index name
    IllegalArgumentException addTableIndexException5 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.AddTableIndexRequest(
                        Index.IndexType.PRIMARY_KEY, null, new String[][] {{"column1"}})
                    .validate());
    Assertions.assertTrue(addTableIndexException5.getMessage().contains("name"));

    // AddTableIndexRequest - empty index name
    IllegalArgumentException addTableIndexException6 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.AddTableIndexRequest(
                        Index.IndexType.PRIMARY_KEY, "", new String[][] {{"column1"}})
                    .validate());
    Assertions.assertTrue(addTableIndexException6.getMessage().contains("name"));

    // AddTableIndexRequest - blank index name
    IllegalArgumentException addTableIndexException7 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.AddTableIndexRequest(
                        Index.IndexType.PRIMARY_KEY, "   ", new String[][] {{"column1"}})
                    .validate());
    Assertions.assertTrue(addTableIndexException7.getMessage().contains("name"));

    // UpdateTableColumnNullabilityRequest - null fieldName
    IllegalArgumentException updateColumnNullabilityException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.UpdateTableColumnNullabilityRequest(null, false).validate());
    Assertions.assertTrue(updateColumnNullabilityException.getMessage().contains("fieldName"));

    // UpdateTableColumnNullabilityRequest - empty fieldName
    IllegalArgumentException updateColumnNullabilityException2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.UpdateTableColumnNullabilityRequest(new String[] {}, false)
                    .validate());
    Assertions.assertTrue(updateColumnNullabilityException2.getMessage().contains("fieldName"));

    // DeleteTableColumnRequest - null fieldName
    IllegalArgumentException deleteTableColumnException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new TableUpdateRequest.DeleteTableColumnRequest(null, false).validate());
    Assertions.assertTrue(deleteTableColumnException.getMessage().contains("fieldName"));

    // DeleteTableColumnRequest - empty fieldName
    IllegalArgumentException deleteTableColumnException2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.DeleteTableColumnRequest(new String[] {}, false).validate());
    Assertions.assertTrue(deleteTableColumnException2.getMessage().contains("fieldName"));

    // UpdateColumnAutoIncrementRequest - null fieldName
    IllegalArgumentException updateColumnAutoIncrementException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new TableUpdateRequest.UpdateColumnAutoIncrementRequest(null, true).validate());
    Assertions.assertTrue(updateColumnAutoIncrementException.getMessage().contains("fieldName"));

    // UpdateColumnAutoIncrementRequest - empty fieldName
    IllegalArgumentException updateColumnAutoIncrementException2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new TableUpdateRequest.UpdateColumnAutoIncrementRequest(new String[] {}, true)
                    .validate());
    Assertions.assertTrue(updateColumnAutoIncrementException2.getMessage().contains("fieldName"));
  }
}
