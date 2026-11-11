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
package org.apache.gravitino.storage.relational.po;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.semantic.SemanticModelDefinitionDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestSemanticModelPO {

  private static final String DEFINITION =
      """
      {
        "datasets": [
          {"name": "orders", "source": {"namespace": ["sales", "mart"], "name": "orders"},
           "primaryKey": [], "aiContext": "Order facts"},
          {"name": "customers", "source": {"namespace": ["sales", "mart"], "name": "customers"}}
        ],
        "relationships": [
          {"name": "orders_customers", "from": "orders", "to": "customers",
           "fromColumns": ["customer_id", "region"], "toColumns": ["id", "region"]}
        ],
        "metrics": [
          {"name": "z_count", "expression": {
            "dialects": [{"dialect": "ANSI_SQL", "expression": "COUNT(*)"}]},
           "aiContext": "Number of orders", "customExtensions": []},
          {"name": "a_revenue", "expression": {
            "dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"}]},
           "datatype": "Decimal"}
        ],
        "aiContext": {
          "instructions": "Use certified definitions", "synonyms": [], "examples": [],
          "threshold": 0.123456789012345678901234567890,
          "nested": {"flags": [true, null], "count": 123456789012345678901234567890}
        },
        "customExtensions": [
          {"vendorName": "z_vendor", "data": "first"},
          {"vendorName": "a_vendor", "data": "second"}
        ]
      }
      """;

  @Test
  void testDefinitionFieldsRoundTrip() throws JsonProcessingException {
    SemanticModelEntity entity = entity(DEFINITION);
    SemanticModelVersionInfoPO snapshot = snapshot(entity);
    JsonNode expected = JsonUtils.anyFieldMapper().readTree(DEFINITION);

    assertEquals(
        expected.get("datasets"), JsonUtils.anyFieldMapper().readTree(snapshot.datasets()));
    assertEquals(
        expected.get("relationships"),
        JsonUtils.anyFieldMapper().readTree(snapshot.relationships()));
    assertEquals(expected.get("metrics"), JsonUtils.anyFieldMapper().readTree(snapshot.metrics()));
    assertEquals(
        expected.get("aiContext"), JsonUtils.anyFieldMapper().readTree(snapshot.aiContext()));
    assertEquals(
        expected.get("customExtensions"),
        JsonUtils.anyFieldMapper().readTree(snapshot.customExtensions()));
    assertEquals(
        entity, SemanticModelPO.fromSemanticModelPO(identity(snapshot), entity.namespace()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testAbsentAndEmptyOptionalFields(boolean explicitEmpty) throws JsonProcessingException {
    ObjectNode definition = (ObjectNode) JsonUtils.anyFieldMapper().readTree(DEFINITION);
    definition.remove("aiContext");
    for (String field : new String[] {"relationships", "metrics", "customExtensions"}) {
      definition.remove(field);
      if (explicitEmpty) {
        definition.putArray(field);
      }
    }
    SemanticModelEntity entity = entity(definition.toString());
    SemanticModelVersionInfoPO snapshot = snapshot(entity);

    assertNull(snapshot.aiContext());
    assertEquals(explicitEmpty ? "[]" : null, snapshot.relationships());
    assertEquals(explicitEmpty ? "[]" : null, snapshot.metrics());
    assertEquals(explicitEmpty ? "[]" : null, snapshot.customExtensions());
    assertEquals(
        entity, SemanticModelPO.fromSemanticModelPO(identity(snapshot), entity.namespace()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"\"Use certified definitions\"", "\"\"", "{}"})
  void testAIContextRepresentations(String context) throws JsonProcessingException {
    ObjectNode definition = (ObjectNode) JsonUtils.anyFieldMapper().readTree(DEFINITION);
    definition.set("aiContext", JsonUtils.anyFieldMapper().readTree(context));
    SemanticModelEntity entity = entity(definition.toString());
    SemanticModelVersionInfoPO snapshot = snapshot(entity);

    assertEquals(context, snapshot.aiContext());
    assertEquals(
        entity, SemanticModelPO.fromSemanticModelPO(identity(snapshot), entity.namespace()));
  }

  @ParameterizedTest
  @CsvSource({
    "datasets, datasets",
    "relationships, relationships",
    "metrics, metrics",
    "aiContext, ai_context",
    "customExtensions, custom_extensions"
  })
  void testCorruptedDefinitionColumnFailsModelLoad(String field, String columnName)
      throws Exception {
    SemanticModelEntity entity = entity(DEFINITION);
    SemanticModelVersionInfoPO snapshot = snapshot(entity);
    FieldUtils.writeField(snapshot, field, "{", true);

    RuntimeException failure =
        assertThrows(
            RuntimeException.class,
            () -> SemanticModelPO.fromSemanticModelPO(identity(snapshot), entity.namespace()));
    assertEquals(
        "Failed to deserialize Semantic Model column '" + columnName + "'", failure.getMessage());
    assertInstanceOf(JsonProcessingException.class, failure.getCause());
  }

  private static SemanticModelEntity entity(String definition) throws JsonProcessingException {
    return SemanticModelEntity.builder()
        .withId(4L)
        .withName("sales_model")
        .withNamespace(Namespace.of("metalake", "catalog", "schema"))
        .withComment("Sales definitions")
        .withDefinition(
            JsonUtils.anyFieldMapper()
                .readValue(definition, SemanticModelDefinitionDTO.class)
                .toDefinition())
        .withProperties(Map.of("owner", "analytics"))
        .withAuditInfo(
            AuditInfo.builder().withCreator("user").withCreateTime(Instant.EPOCH).build())
        .build();
  }

  private static SemanticModelVersionInfoPO snapshot(SemanticModelEntity entity) {
    return SemanticModelPO.initializeSemanticModelVersionInfoPO(
        entity, new NamespacedEntityId(3L, 1L, 2L), 1);
  }

  private static SemanticModelPO identity(SemanticModelVersionInfoPO snapshot) {
    return SemanticModelPO.builder()
        .withSemanticModelId(snapshot.semanticModelId())
        .withSemanticModelName(snapshot.semanticModelName())
        .withMetalakeId(snapshot.metalakeId())
        .withCatalogId(snapshot.catalogId())
        .withSchemaId(snapshot.schemaId())
        .withAuditInfo(snapshot.auditInfo())
        .withCurrentVersion(snapshot.version())
        .withLastVersion(snapshot.version())
        .withDeletedAt(snapshot.deletedAt())
        .withSemanticModelVersionInfoPO(snapshot)
        .build();
  }
}
