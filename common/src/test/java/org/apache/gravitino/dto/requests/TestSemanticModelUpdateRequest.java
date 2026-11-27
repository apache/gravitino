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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.junit.jupiter.api.Test;

public class TestSemanticModelUpdateRequest {

  @Test
  public void testAllUpdateTypesSerDeValidateAndConvert() throws JsonProcessingException {
    String json =
        """
        {"expectedRevision": 4, "updates":[
          {"@type":"updateComment","newComment":null},
          {"@type":"setProperty","property":"owner","value":"finance"},
          {"@type":"removeProperty","property":"legacy"},
          {"@type":"replaceDefinition","definition":{
            "datasets":[{"name":"orders","source":{"namespace":["sales","mart"],"name":"orders"}}],
            "relationships":[],"metrics":[],"customExtensions":[]}}
        ]}
        """;
    SemanticModelUpdatesRequest request =
        JsonUtils.objectMapper().readValue(json, SemanticModelUpdatesRequest.class);
    request.validate();
    assertEquals(4, request.getExpectedRevision());

    SemanticModelChange.UpdateComment updateComment =
        assertInstanceOf(
            SemanticModelChange.UpdateComment.class,
            request.getUpdates().get(0).semanticModelChange());
    assertNull(updateComment.getNewComment());
    SemanticModelChange.SetProperty setProperty =
        assertInstanceOf(
            SemanticModelChange.SetProperty.class,
            request.getUpdates().get(1).semanticModelChange());
    assertEquals("owner", setProperty.getProperty());
    assertEquals("finance", setProperty.getValue());
    SemanticModelChange.RemoveProperty removeProperty =
        assertInstanceOf(
            SemanticModelChange.RemoveProperty.class,
            request.getUpdates().get(2).semanticModelChange());
    assertEquals("legacy", removeProperty.getProperty());
    SemanticModelChange.ReplaceDefinition replaceDefinition =
        assertInstanceOf(
            SemanticModelChange.ReplaceDefinition.class,
            request.getUpdates().get(3).semanticModelChange());
    assertEquals("orders", replaceDefinition.getDefinition().datasets()[0].name());
    assertEquals(0, replaceDefinition.getDefinition().relationships().length);

    JsonNode serialized =
        JsonUtils.objectMapper().readTree(JsonUtils.objectMapper().writeValueAsString(request));
    assertEquals(4, serialized.path("expectedRevision").asInt());
    assertEquals("updateComment", serialized.path("updates").get(0).path("@type").textValue());
    assertEquals("setProperty", serialized.path("updates").get(1).path("@type").textValue());
    assertEquals("removeProperty", serialized.path("updates").get(2).path("@type").textValue());
    assertEquals("replaceDefinition", serialized.path("updates").get(3).path("@type").textValue());
    assertEquals(
        "orders",
        serialized
            .path("updates")
            .get(3)
            .path("definition")
            .path("datasets")
            .get(0)
            .path("name")
            .textValue());
  }

  @Test
  void testRenameRequestRoundTrip() throws JsonProcessingException {
    String json = "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"sales_v2\"}]}";
    SemanticModelUpdatesRequest request =
        JsonUtils.objectMapper().readValue(json, SemanticModelUpdatesRequest.class);
    request.validate();
    SemanticModelChange.RenameSemanticModel rename =
        assertInstanceOf(
            SemanticModelChange.RenameSemanticModel.class,
            request.getUpdates().get(0).semanticModelChange());
    assertEquals("sales_v2", rename.getNewName());
    assertEquals(
        JsonUtils.objectMapper().readTree(json), JsonUtils.objectMapper().valueToTree(request));
  }

  @Test
  public void testUpdatesWrapperRequiresNonEmptyValidatedItems() {
    IllegalArgumentException missing =
        assertThrows(IllegalArgumentException.class, new SemanticModelUpdatesRequest()::validate);
    assertEquals("updates must not be null", missing.getMessage());

    IllegalArgumentException empty =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SemanticModelUpdatesRequest(Collections.emptyList()).validate());
    assertEquals("updates must not be empty", empty.getMessage());

    IllegalArgumentException nullItem =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new SemanticModelUpdatesRequest(Arrays.asList((SemanticModelUpdateRequest) null))
                    .validate());
    assertEquals("update must not be null", nullItem.getMessage());

    IllegalArgumentException invalidRename =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new SemanticModelUpdatesRequest(
                        Collections.singletonList(
                            new SemanticModelUpdateRequest.RenameSemanticModelRequest(" ")))
                    .validate());
    assertEquals("\"newName\" field is required and cannot be empty", invalidRename.getMessage());
  }

  @Test
  public void testCommentValuesAndReplacementValidation() throws JsonProcessingException {
    SemanticModelUpdateRequest.UpdateSemanticModelCommentRequest emptyComment =
        new SemanticModelUpdateRequest.UpdateSemanticModelCommentRequest("");
    emptyComment.validate();
    assertEquals(
        "",
        ((SemanticModelChange.UpdateComment) emptyComment.semanticModelChange()).getNewComment());

    String nullDataset =
        "{\"expectedRevision\":1,\"updates\":[{\"@type\":\"replaceDefinition\","
            + "\"definition\":{\"datasets\":[null]}}]}";
    SemanticModelUpdatesRequest request =
        JsonUtils.objectMapper().readValue(nullDataset, SemanticModelUpdatesRequest.class);
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, request::validate);
    assertEquals("datasets[0] must not be null", exception.getMessage());
  }

  @Test
  public void testReplaceDefinitionRejectsUnknownNestedFields() {
    String json =
        "{\"expectedRevision\":1,\"updates\":[{\"@type\":\"replaceDefinition\",\"definition\":{"
            + "\"datasets\":[{\"name\":\"orders\","
            + "\"source\":{\"namespace\":[\"sales\",\"mart\"],\"name\":\"orders\"}}],"
            + "\"unknown\":true}}]}";

    assertThrows(
        JsonProcessingException.class,
        () -> JsonUtils.objectMapper().readValue(json, SemanticModelUpdatesRequest.class));
  }
}
