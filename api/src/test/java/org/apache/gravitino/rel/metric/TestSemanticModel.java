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
package org.apache.gravitino.rel.metric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Test;

public class TestSemanticModel {

  @Test
  public void testBuildCompleteSemanticModel() {
    DialectExpression dialectExpression =
        DialectExpression.builder()
            .withDialect(MetricDialects.ANSI_SQL)
            .withExpression("orders.amount")
            .build();
    Expression expression =
        Expression.builder().withDialects(Collections.singletonList(dialectExpression)).build();
    AIContext aiContext =
        AIContext.builder()
            .withInstructions("Use certified fields")
            .withSynonyms(Collections.singletonList("sales"))
            .withExamples(Collections.singletonList("Revenue by month"))
            .withAdditionalProperties(Collections.singletonMap("audience", "finance"))
            .build();
    CustomExtension extension =
        CustomExtension.builder().withVendorName("example").withData("{\"key\":1}").build();
    Field field =
        Field.builder()
            .withName("amount")
            .withExpression(expression)
            .withDimension(Dimension.builder().withTime(false).build())
            .withLabel("Amount")
            .withAIContext(aiContext)
            .withCustomExtensions(Collections.singletonList(extension))
            .build();
    Dataset orders =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withPrimaryKey(Collections.singletonList("order_id"))
            .withUniqueKeys(
                Collections.singletonList(Collections.singletonList("external_order_id")))
            .withFields(Collections.singletonList(field))
            .build();
    Dataset customers =
        Dataset.builder()
            .withName("customers")
            .withSource(NameIdentifier.of("sales", "mart", "customers"))
            .build();
    Relationship relationship =
        Relationship.builder()
            .withName("orders_to_customers")
            .withFrom("orders")
            .withTo("customers")
            .withFromColumns(Collections.singletonList("customer_id"))
            .withToColumns(Collections.singletonList("customer_id"))
            .build();
    Metric metric =
        Metric.builder()
            .withName("revenue")
            .withExpression(expression)
            .withDescription("Gross revenue")
            .build();

    SemanticModel model =
        SemanticModel.builder()
            .withName("sales_semantic_model")
            .withDescription("Sales semantics")
            .withAIContext(aiContext)
            .withDatasets(java.util.Arrays.asList(orders, customers))
            .withRelationships(Collections.singletonList(relationship))
            .withMetrics(Collections.singletonList(metric))
            .withCustomExtensions(Collections.singletonList(extension))
            .build();

    assertEquals("sales_semantic_model", model.name());
    assertEquals("Sales semantics", model.description());
    assertEquals(aiContext, model.aiContext());
    assertEquals(java.util.Arrays.asList(orders, customers), model.datasets());
    assertEquals(Collections.singletonList(relationship), model.relationships());
    assertEquals(Collections.singletonList(metric), model.metrics());
    assertEquals(Collections.singletonList(extension), model.customExtensions());
    assertEquals(NameIdentifier.of("sales", "mart", "orders"), orders.source());
    assertEquals(Collections.singletonList("order_id"), orders.primaryKey());
    assertEquals(Collections.singletonList(field), orders.fields());
    assertEquals(MetricDialects.ANSI_SQL, dialectExpression.dialect());
    assertEquals("orders.amount", dialectExpression.expression());
  }

  @Test
  public void testCollectionsAreImmutableCopies() {
    List<Dataset> datasets = new ArrayList<>();
    datasets.add(testDataset("orders"));
    SemanticModel model = SemanticModel.builder().withName("sales").withDatasets(datasets).build();

    datasets.add(testDataset("customers"));

    assertEquals(1, model.datasets().size());
    assertThrows(UnsupportedOperationException.class, () -> model.datasets().clear());
  }

  @Test
  public void testRejectInvalidRequiredFields() {
    assertThrows(IllegalArgumentException.class, () -> SemanticModel.builder().build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticModel.builder()
                .withName("sales")
                .withDatasets(Collections.emptyList())
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () -> Dataset.builder().withName("orders").withSource(NameIdentifier.of("orders")).build());
  }

  @Test
  public void testRejectMismatchedRelationshipColumns() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            Relationship.builder()
                .withName("orders_to_customers")
                .withFrom("orders")
                .withTo("customers")
                .withFromColumns(Collections.singletonList("customer_id"))
                .withToColumns(java.util.Arrays.asList("customer_id", "tenant_id"))
                .build());
  }

  private static Dataset testDataset(String name) {
    return Dataset.builder()
        .withName(name)
        .withSource(NameIdentifier.of("sales", "mart", name))
        .build();
  }
}
