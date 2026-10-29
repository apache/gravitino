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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.SemanticModelMetric;
import org.junit.jupiter.api.Test;

class TestGenericSemanticModelMetric {

  @Test
  void testValueEqualityAndCollections() {
    SemanticModelMetric first =
        new GenericSemanticModelMetric(
            Namespace.of("schema", "sales_model"), metric("SUM(orders.amount)", "Total revenue"));
    SemanticModelMetric second =
        new GenericSemanticModelMetric(
            Namespace.of("schema", "sales_model"), metric("SUM(orders.amount)", "Total revenue"));

    assertEquals(first, first);
    assertEquals(first, second);
    assertEquals(second, first);
    assertEquals(first.hashCode(), second.hashCode());
    assertNotEquals(first, null);
    assertNotEquals(first, new Object());
    assertTrue(List.of(first).contains(second));

    Set<SemanticModelMetric> metrics = new HashSet<>();
    assertTrue(metrics.add(first));
    assertFalse(metrics.add(second));
    assertEquals(1, metrics.size());
    assertTrue(metrics.contains(second));
  }

  @Test
  void testEqualityIncludesNamespaceAndCompleteDefinition() {
    Namespace namespace = Namespace.of("schema", "sales_model");
    Metric metric = metric("SUM(orders.amount)", "Total revenue");
    SemanticModelMetric original = new GenericSemanticModelMetric(namespace, metric);

    assertNotEquals(
        original, new GenericSemanticModelMetric(Namespace.of("schema", "other_model"), metric));
    assertNotEquals(
        original,
        new GenericSemanticModelMetric(namespace, metric("COUNT(orders.amount)", "Total revenue")));
    assertNotEquals(
        original,
        new GenericSemanticModelMetric(
            namespace, metric("SUM(orders.amount)", "Updated description")));
  }

  @Test
  void testToStringIncludesNamespaceAndMetricDefinition() {
    SemanticModelMetric info =
        new GenericSemanticModelMetric(
            Namespace.of("schema", "sales_model"), metric("SUM(orders.amount)", "Total revenue"));

    String description = info.toString();
    assertTrue(description.contains("namespace=schema.sales_model"));
    assertTrue(description.contains("name='revenue'"));
    assertTrue(description.contains("SUM(orders.amount)"));
    assertTrue(description.contains("Total revenue"));
  }

  private static Metric metric(String expression, String description) {
    return Metric.builder()
        .withName("revenue")
        .withExpression(
            Expression.builder()
                .withDialects(
                    new DialectExpression[] {
                      DialectExpression.builder()
                          .withDialect("ANSI_SQL")
                          .withExpression(expression)
                          .build()
                    })
                .build())
        .withDescription(description)
        .build();
  }
}
