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
package org.apache.gravitino.semantic;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import org.apache.gravitino.Audit;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Test;

public class TestSemanticModelDefinition {

  @Test
  public void testBuildDefinitionAndDefensivelyCopyCollections() {
    Dataset dataset = dataset("orders");
    Relationship relationship =
        Relationship.builder()
            .withName("orders_to_customers")
            .withFrom("orders")
            .withTo("customers")
            .withFromColumns(new String[] {"customer_id"})
            .withToColumns(new String[] {"id"})
            .build();
    Metric metric =
        Metric.builder().withName("count").withExpression(expression("COUNT(*)")).build();
    CustomExtension extension =
        CustomExtension.builder().withVendorName("example").withData("{}").build();
    Dataset[] datasets = {dataset};
    Relationship[] relationships = {relationship};
    Metric[] metrics = {metric};
    CustomExtension[] extensions = {extension};

    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withAIContext(AIContext.of("Certified"))
            .withDatasets(datasets)
            .withRelationships(relationships)
            .withMetrics(metrics)
            .withCustomExtensions(extensions)
            .build();

    datasets[0] = dataset("changed");
    relationships[0] = relationship;
    metrics[0] = metric;
    extensions[0] = extension;

    assertEquals("Certified", definition.aiContext().text());
    assertEquals("orders", definition.datasets()[0].name());
    assertArrayEquals(new Relationship[] {relationship}, definition.relationships());
    assertArrayEquals(new Metric[] {metric}, definition.metrics());
    assertArrayEquals(new CustomExtension[] {extension}, definition.customExtensions());
    assertNotSame(definition.datasets(), definition.datasets());
  }

  @Test
  public void testOptionalCollectionsPreserveNullAndEmpty() {
    Dataset dataset = dataset("orders");
    SemanticModelDefinition unset =
        SemanticModelDefinition.builder().withDatasets(new Dataset[] {dataset}).build();
    SemanticModelDefinition empty =
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {dataset})
            .withRelationships(new Relationship[0])
            .withMetrics(new Metric[0])
            .withCustomExtensions(new CustomExtension[0])
            .build();
    SemanticModel semanticModel = semanticModel(unset);

    assertNull(unset.relationships());
    assertNull(unset.metrics());
    assertNull(unset.customExtensions());
    assertArrayEquals(new Relationship[0], empty.relationships());
    assertArrayEquals(new Metric[0], empty.metrics());
    assertArrayEquals(new CustomExtension[0], empty.customExtensions());
    assertNotEquals(unset, empty);
    assertEquals(unset, semanticModel.definition());
    assertEquals(Collections.emptyMap(), semanticModel.properties());
  }

  @Test
  public void testDefinitionRequiresDatasets() {
    assertThrows(IllegalArgumentException.class, () -> SemanticModelDefinition.builder().build());
    assertThrows(
        IllegalArgumentException.class,
        () -> SemanticModelDefinition.builder().withDatasets(new Dataset[0]).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> SemanticModelDefinition.builder().withDatasets(new Dataset[] {null}).build());

    Dataset dataset = dataset("orders");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticModelDefinition.builder()
                .withDatasets(new Dataset[] {dataset})
                .withRelationships(new Relationship[] {null})
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticModelDefinition.builder()
                .withDatasets(new Dataset[] {dataset})
                .withMetrics(new Metric[] {null})
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticModelDefinition.builder()
                .withDatasets(new Dataset[] {dataset})
                .withCustomExtensions(new CustomExtension[] {null})
                .build());
  }

  private static Dataset dataset(String name) {
    return Dataset.builder()
        .withName(name)
        .withSource(NameIdentifier.of("sales", "mart", name))
        .build();
  }

  private static Expression expression(String value) {
    return Expression.builder()
        .withDialects(
            new DialectExpression[] {
              DialectExpression.builder()
                  .withDialect(Dialects.ANSI_SQL)
                  .withExpression(value)
                  .build()
            })
        .build();
  }

  private static SemanticModel semanticModel(SemanticModelDefinition definition) {
    return new SemanticModel() {
      @Override
      public String name() {
        return "sales";
      }

      @Override
      public String comment() {
        return null;
      }

      @Override
      public SemanticModelDefinition definition() {
        return definition;
      }

      @Override
      public Audit auditInfo() {
        return null;
      }
    };
  }
}
