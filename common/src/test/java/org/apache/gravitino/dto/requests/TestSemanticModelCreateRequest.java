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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.semantic.SemanticModelDefinitionDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.junit.jupiter.api.Test;

public class TestSemanticModelCreateRequest {

  @Test
  public void testWrappedDefinitionJsonAndConversion() throws JsonProcessingException {
    String json =
        "{"
            + "\"name\":\"sales_model\","
            + "\"comment\":\"Governed sales definitions\","
            + "\"definition\":{"
            + "\"ai_context\":\"Use governed metrics\","
            + "\"datasets\":[{\"name\":\"orders\","
            + "\"source\":{\"namespace\":[\"sales\",\"mart\"],\"name\":\"orders\"}}],"
            + "\"relationships\":[],"
            + "\"metrics\":[],"
            + "\"custom_extensions\":[]},"
            + "\"properties\":{}"
            + "}";

    SemanticModelCreateRequest request =
        JsonUtils.objectMapper().readValue(json, SemanticModelCreateRequest.class);
    request.validate();
    SemanticModelDefinition definition = request.toDefinition();

    assertEquals("sales_model", request.getName());
    assertEquals("Use governed metrics", definition.aiContext().text());
    assertEquals(1, definition.datasets().length);
    assertEquals("orders", definition.datasets()[0].name());
    assertEquals(0, definition.relationships().length);
    assertEquals(0, definition.metrics().length);
    assertEquals(0, definition.customExtensions().length);

    JsonNode serialized =
        JsonUtils.objectMapper().readTree(JsonUtils.objectMapper().writeValueAsString(request));
    assertTrue(serialized.has("definition"));
    assertTrue(serialized.path("definition").has("ai_context"));
    assertTrue(serialized.path("definition").has("datasets"));
    assertTrue(serialized.has("properties"));
    assertFalse(serialized.has("datasets"));
  }

  @Test
  public void testRequiredFieldValidation() {
    SemanticModelDefinitionDTO definition =
        SemanticModelDefinitionDTO.fromDefinition(
            SemanticModelDefinition.builder().withDatasets(new Dataset[] {dataset()}).build());

    IllegalArgumentException missingName =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SemanticModelCreateRequest(null, null, definition, Map.of()).validate());
    assertEquals("\"name\" field is required and cannot be empty", missingName.getMessage());

    IllegalArgumentException missingDefinition =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SemanticModelCreateRequest("sales_model", null, null, Map.of()).validate());
    assertEquals(
        "\"definition\" field is required and cannot be null", missingDefinition.getMessage());

    IllegalArgumentException missingProperties =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SemanticModelCreateRequest("sales_model", null, definition, null).validate());
    assertEquals(
        "\"properties\" field is required and cannot be null", missingProperties.getMessage());
  }

  @Test
  public void testUnknownDefinitionFieldsAreRejected() {
    String unknownTopLevel =
        "{"
            + "\"name\":\"sales_model\","
            + "\"definition\":{\"datasets\":[{\"name\":\"orders\","
            + "\"source\":{\"namespace\":[\"sales\",\"mart\"],\"name\":\"orders\"}}]},"
            + "\"properties\":{},"
            + "\"unknown\":true"
            + "}";
    String unknownDataset =
        "{"
            + "\"name\":\"sales_model\","
            + "\"definition\":{\"datasets\":[{\"name\":\"orders\","
            + "\"source\":{\"namespace\":[\"sales\",\"mart\"],\"name\":\"orders\"},"
            + "\"unknown\":true}]},"
            + "\"properties\":{}"
            + "}";

    assertThrows(
        JsonProcessingException.class,
        () ->
            JsonUtils.objectMapper().readValue(unknownTopLevel, SemanticModelCreateRequest.class));
    assertThrows(
        JsonProcessingException.class,
        () -> JsonUtils.objectMapper().readValue(unknownDataset, SemanticModelCreateRequest.class));
  }

  @Test
  public void testNestedNullMembersAreRejected() throws JsonProcessingException {
    String json =
        "{"
            + "\"name\":\"sales_model\","
            + "\"definition\":{\"datasets\":[null]},"
            + "\"properties\":{}"
            + "}";
    SemanticModelCreateRequest request =
        JsonUtils.objectMapper().readValue(json, SemanticModelCreateRequest.class);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, request::validate);
    assertEquals("datasets[0] must not be null", exception.getMessage());
  }

  private static Dataset dataset() {
    return Dataset.builder()
        .withName("orders")
        .withSource(NameIdentifier.of("sales", "mart", "orders"))
        .build();
  }
}
