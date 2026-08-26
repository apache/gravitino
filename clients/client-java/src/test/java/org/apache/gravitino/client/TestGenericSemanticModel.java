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
package org.apache.gravitino.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.Field;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.junit.jupiter.api.Test;

public class TestGenericSemanticModel {

  @Test
  public void testNestedDefinitionAndDefensiveState() {
    Dataset absent = dataset("absent");
    Dataset explicitEmpty =
        Dataset.builder()
            .withName("explicit_empty")
            .withSource(NameIdentifier.of("source_catalog", "source_schema", "explicit_empty"))
            .withPrimaryKey(new String[0])
            .withUniqueKeys(new String[0][])
            .withFields(new Field[0])
            .withCustomExtensions(new CustomExtension[0])
            .build();
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {absent, explicitEmpty})
            .withRelationships(new Relationship[0])
            .withCustomExtensions(new CustomExtension[0])
            .build();
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("certified", "true");
    AuditDTO audit =
        AuditDTO.builder()
            .withCreator("creator")
            .withCreateTime(Instant.parse("2026-08-11T00:00:00Z"))
            .build();
    SemanticModel source = semanticModel(definition, properties, audit);

    GenericSemanticModel semanticModel = new GenericSemanticModel(source);
    GenericSemanticModel equalModel = new GenericSemanticModel(source);
    properties.put("changed", "true");

    assertEquals("sales_model", semanticModel.name());
    assertEquals("Sales model", semanticModel.comment());
    assertSame(definition, semanticModel.definition());
    assertArrayEquals(new Dataset[] {absent, explicitEmpty}, semanticModel.definition().datasets());
    assertArrayEquals(new Relationship[0], semanticModel.definition().relationships());
    assertNull(semanticModel.definition().metrics());
    assertArrayEquals(new CustomExtension[0], semanticModel.definition().customExtensions());
    assertNull(semanticModel.definition().datasets()[0].fields());
    assertArrayEquals(new Field[0], semanticModel.definition().datasets()[1].fields());
    assertEquals(Map.of("certified", "true"), semanticModel.properties());
    assertSame(audit, semanticModel.auditInfo());

    Dataset[] returnedDatasets = semanticModel.definition().datasets();
    returnedDatasets[0] = explicitEmpty;
    assertArrayEquals(new Dataset[] {absent, explicitEmpty}, semanticModel.definition().datasets());
    assertThrows(
        UnsupportedOperationException.class, () -> semanticModel.properties().put("new", "value"));

    assertEquals(semanticModel, equalModel);
    assertEquals(semanticModel.hashCode(), equalModel.hashCode());
  }

  private static SemanticModel semanticModel(
      SemanticModelDefinition definition, Map<String, String> properties, Audit audit) {
    return new SemanticModel() {
      @Override
      public String name() {
        return "sales_model";
      }

      @Override
      public String comment() {
        return "Sales model";
      }

      @Override
      public SemanticModelDefinition definition() {
        return definition;
      }

      @Override
      public Map<String, String> properties() {
        return properties;
      }

      @Override
      public Audit auditInfo() {
        return audit;
      }
    };
  }

  private static Dataset dataset(String name) {
    return Dataset.builder()
        .withName(name)
        .withSource(NameIdentifier.of("source_catalog", "source_schema", name))
        .build();
  }
}
