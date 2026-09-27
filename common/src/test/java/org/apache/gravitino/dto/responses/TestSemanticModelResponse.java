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
package org.apache.gravitino.dto.responses;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.semantic.SemanticModelDTO;
import org.apache.gravitino.dto.semantic.SemanticModelDefinitionDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.junit.jupiter.api.Test;

public class TestSemanticModelResponse {

  @Test
  public void testResponseSerDeAndValidate() throws JsonProcessingException {
    SemanticModelResponse response = new SemanticModelResponse(semanticModelDTO());

    String json = JsonUtils.objectMapper().writeValueAsString(response);
    JsonNode root = JsonUtils.objectMapper().readTree(json);
    JsonNode semanticModel = root.path("semanticModel");
    assertTrue(root.has("semanticModel"));
    assertFalse(root.has("semantic_model"));
    assertTrue(semanticModel.has("definition"));
    assertTrue(semanticModel.path("definition").has("datasets"));
    assertFalse(semanticModel.has("datasets"));

    SemanticModelResponse deserialized =
        JsonUtils.objectMapper().readValue(json, SemanticModelResponse.class);
    deserialized.validate();
    assertEquals("sales_model", deserialized.getSemanticModel().name());
    assertEquals("orders", deserialized.getSemanticModel().definition().datasets()[0].name());
  }

  @Test
  public void testResponseRequiresSemanticModel() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, new SemanticModelResponse()::validate);
    assertEquals("semanticModel must not be null", exception.getMessage());
  }

  @Test
  public void testResponseValidatesSemanticModelMembers() throws JsonProcessingException {
    String definition =
        "{\"datasets\":[{\"name\":\"orders\",\"source\":{"
            + "\"namespace\":[\"sales\",\"mart\"],\"name\":\"orders\"}}]}";
    SemanticModelResponse missingName =
        readResponse(
            "{\"code\":0,\"semanticModel\":{\"definition\":" + definition + ",\"audit\":{}}}");
    SemanticModelResponse missingDefinition =
        readResponse("{\"code\":0,\"semanticModel\":{\"name\":\"sales_model\",\"audit\":{}}}");
    SemanticModelResponse missingAudit =
        readResponse(
            "{\"code\":0,\"semanticModel\":{\"name\":\"sales_model\",\"definition\":"
                + definition
                + "}}");

    assertEquals(
        "semanticModel 'name' must not be null or empty",
        assertThrows(IllegalArgumentException.class, missingName::validate).getMessage());
    assertEquals(
        "definition must not be null",
        assertThrows(IllegalArgumentException.class, missingDefinition::validate).getMessage());
    assertEquals(
        "semanticModel 'audit' must not be null",
        assertThrows(IllegalArgumentException.class, missingAudit::validate).getMessage());
  }

  private static SemanticModelDTO semanticModelDTO() {
    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .build();
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder().withDatasets(new Dataset[] {dataset}).build();
    AuditDTO audit =
        AuditDTO.builder()
            .withCreator("tester")
            .withCreateTime(Instant.parse("2026-08-11T00:00:00Z"))
            .build();
    return SemanticModelDTO.builder()
        .withName("sales_model")
        .withDefinition(SemanticModelDefinitionDTO.fromDefinition(definition))
        .withProperties(Map.of())
        .withAudit(audit)
        .build();
  }

  private static SemanticModelResponse readResponse(String json) throws JsonProcessingException {
    return JsonUtils.objectMapper().readValue(json, SemanticModelResponse.class);
  }
}
