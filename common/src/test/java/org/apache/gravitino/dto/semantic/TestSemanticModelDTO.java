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
package org.apache.gravitino.dto.semantic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.junit.jupiter.api.Test;

public class TestSemanticModelDTO {

  @Test
  public void testNestedDefinitionJsonRoundTrip() throws JsonProcessingException {
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withAIContext(AIContext.of("Use governed metrics"))
            .withDatasets(new Dataset[] {dataset("orders")})
            .withRelationships(new Relationship[0])
            .withMetrics(new Metric[0])
            .withCustomExtensions(new CustomExtension[0])
            .build();
    SemanticModelDTO dto =
        SemanticModelDTO.builder()
            .withName("sales_model")
            .withComment("Governed sales definitions")
            .withDefinition(SemanticModelDefinitionDTO.fromDefinition(definition))
            .withProperties(Map.of("owner", "finance"))
            .withAudit(audit())
            .build();

    String json = JsonUtils.objectMapper().writeValueAsString(dto);
    JsonNode root = JsonUtils.objectMapper().readTree(json);

    assertEquals("sales_model", root.path("name").textValue());
    assertTrue(root.has("definition"));
    assertEquals("Use governed metrics", root.path("definition").path("ai_context").textValue());
    assertTrue(root.path("definition").has("datasets"));
    assertTrue(root.path("definition").has("relationships"));
    assertTrue(root.has("properties"));
    assertTrue(root.has("audit"));
    assertFalse(root.has("datasets"));

    SemanticModelDTO deserialized =
        JsonUtils.objectMapper().readValue(json, SemanticModelDTO.class);
    assertEquals("sales_model", deserialized.name());
    assertEquals("Governed sales definitions", deserialized.comment());
    assertEquals(definition, deserialized.definition());
    assertEquals(Map.of("owner", "finance"), deserialized.properties());
    assertNotNull(deserialized.auditInfo());
  }

  @Test
  public void testDefinitionAndPropertiesAreDefensive() {
    Dataset orders = dataset("orders");
    DatasetDTO[] datasets = {DatasetDTO.fromDataset(orders)};
    SemanticModelDefinitionDTO definition =
        SemanticModelDefinitionDTO.builder().withDatasets(datasets).build();
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("owner", "finance");

    SemanticModelDTO dto =
        SemanticModelDTO.builder()
            .withName("sales_model")
            .withDefinition(definition)
            .withProperties(properties)
            .withAudit(audit())
            .build();

    datasets[0] = DatasetDTO.fromDataset(dataset("customers"));
    properties.put("owner", "sales");
    Dataset[] returnedDatasets = dto.definition().datasets();
    returnedDatasets[0] = dataset("customers");

    assertEquals(orders, dto.definition().datasets()[0]);
    assertEquals(Map.of("owner", "finance"), dto.properties());
    assertThrows(UnsupportedOperationException.class, () -> dto.properties().put("k", "v"));
  }

  @Test
  public void testAbsentDefinitionCollectionsRemainAbsent() throws JsonProcessingException {
    String json =
        "{"
            + "\"name\":\"sales_model\","
            + "\"definition\":{\"datasets\":[{\"name\":\"orders\","
            + "\"source\":{\"namespace\":[\"sales\",\"mart\"],\"name\":\"orders\"}}]},"
            + "\"audit\":{}"
            + "}";

    SemanticModelDTO dto = JsonUtils.objectMapper().readValue(json, SemanticModelDTO.class);
    assertNull(dto.definition().relationships());
    assertNull(dto.definition().metrics());
    assertNull(dto.definition().customExtensions());
    assertTrue(dto.properties().isEmpty());

    String unknown = json.substring(0, json.length() - 1) + ",\"unknown\":true}";
    assertThrows(
        JsonProcessingException.class,
        () -> JsonUtils.objectMapper().readValue(unknown, SemanticModelDTO.class));
  }

  @Test
  public void testBuilderValidation() {
    SemanticModelDefinitionDTO definition = definitionDTO("orders");
    IllegalArgumentException missingName =
        assertThrows(
            IllegalArgumentException.class,
            () -> SemanticModelDTO.builder().withDefinition(definition).withAudit(audit()).build());
    assertEquals("name cannot be null or empty", missingName.getMessage());

    IllegalArgumentException missingDefinition =
        assertThrows(
            IllegalArgumentException.class,
            () -> SemanticModelDTO.builder().withName("sales_model").withAudit(audit()).build());
    assertEquals("definition cannot be null", missingDefinition.getMessage());

    SemanticModelDefinitionDTO invalidDefinition =
        SemanticModelDefinitionDTO.builder().withDatasets(new DatasetDTO[] {null}).build();
    IllegalArgumentException nullDataset =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SemanticModelDTO.builder()
                    .withName("sales_model")
                    .withDefinition(invalidDefinition)
                    .withAudit(audit())
                    .build());
    assertEquals("datasets[0] must not be null", nullDataset.getMessage());
  }

  private static SemanticModelDefinitionDTO definitionDTO(String datasetName) {
    return SemanticModelDefinitionDTO.fromDefinition(
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {dataset(datasetName)})
            .build());
  }

  private static Dataset dataset(String name) {
    return Dataset.builder()
        .withName(name)
        .withSource(NameIdentifier.of("sales", "mart", name))
        .build();
  }

  private static AuditDTO audit() {
    return AuditDTO.builder()
        .withCreator("tester")
        .withCreateTime(Instant.parse("2026-08-11T00:00:00Z"))
        .build();
  }
}
