/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.types.Types;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import java.util.List;
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
            + "      \"newName\": \"newTable\"\n"
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
  }

  @Test
  public void testAddTableColumnRequest() throws JsonProcessingException {
    TableUpdateRequest addTableColumnRequest =
        new TableUpdateRequest.AddTableColumnRequest(
            new String[] {"column"},
            Types.StringType.get(),
            "comment",
            TableChange.ColumnPosition.after("afterColumn"),
            false);
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
            + "  \"nullable\": false\n"
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
            + "  \"nullable\": true\n"
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
            + "  \"nullable\": true\n"
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
}
