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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.semantic.SemanticQueryFilter.Operator;
import org.junit.jupiter.api.Test;

public class TestSemanticQuery {

  @Test
  public void testBuildQuery() {
    SemanticQueryFilter filter = SemanticQueryFilter.of("order_date", Operator.GE, "2024-01-01");
    SemanticQuery query =
        SemanticQuery.builder()
            .withMetrics(Arrays.asList("order_amount", "order_count"))
            .withDimensions(Collections.singletonList("region"))
            .withFilters(Collections.singletonList(filter))
            .withDialect("trino")
            .build();

    assertEquals(Arrays.asList("order_amount", "order_count"), query.metrics());
    assertEquals(Collections.singletonList("region"), query.dimensions());
    assertEquals(Collections.singletonList(filter), query.filters());
    assertEquals("trino", query.dialect());
  }

  @Test
  public void testQueryRequiresMetricOrDimension() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticQuery.builder()
                .withMetrics(Collections.emptyList())
                .withDimensions(Collections.emptyList())
                .withFilters(Collections.emptyList())
                .withDialect("trino")
                .build());
  }

  @Test
  public void testQueryRequiresNonNullCollections() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticQuery.builder()
                .withMetrics(null)
                .withDimensions(Collections.emptyList())
                .withFilters(Collections.emptyList())
                .withDialect("trino")
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticQuery.builder()
                .withMetrics(Collections.singletonList("metric"))
                .withDimensions(null)
                .withFilters(Collections.emptyList())
                .withDialect("trino")
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticQuery.builder()
                .withMetrics(Collections.singletonList("metric"))
                .withDimensions(Collections.emptyList())
                .withFilters(null)
                .withDialect("trino")
                .build());
  }

  @Test
  public void testQueryRejectsBlankDialect() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticQuery.builder()
                .withMetrics(Collections.singletonList("metric"))
                .withDimensions(Collections.emptyList())
                .withFilters(Collections.emptyList())
                .withDialect(" ")
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticQuery.builder()
                .withMetrics(Collections.singletonList("metric"))
                .withDimensions(Collections.emptyList())
                .withFilters(Collections.emptyList())
                .withDialect(null)
                .build());
  }

  @Test
  public void testQueryRejectsBlankNamesAndNullFilters() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticQuery.builder()
                .withMetrics(Collections.singletonList(" "))
                .withDimensions(Collections.emptyList())
                .withFilters(Collections.emptyList())
                .withDialect("trino")
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticQuery.builder()
                .withMetrics(Collections.singletonList("metric"))
                .withDimensions(Collections.singletonList(" "))
                .withFilters(Collections.emptyList())
                .withDialect("trino")
                .build());
    List<SemanticQueryFilter> filters = new ArrayList<>();
    filters.add(null);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticQuery.builder()
                .withMetrics(Collections.singletonList("metric"))
                .withDimensions(Collections.emptyList())
                .withFilters(filters)
                .withDialect("trino")
                .build());
  }

  @Test
  public void testQueryIsImmutable() {
    SemanticQuery query =
        SemanticQuery.builder()
            .withMetrics(new ArrayList<>(Collections.singletonList("metric")))
            .withDimensions(Collections.emptyList())
            .withFilters(Collections.emptyList())
            .withDialect("trino")
            .build();

    assertThrows(UnsupportedOperationException.class, () -> query.metrics().add("other"));
  }

  @Test
  public void testQueryEquality() {
    SemanticQuery first = query("metric", "trino");
    SemanticQuery same = query("metric", "trino");
    SemanticQuery otherDialect = query("metric", "spark");

    assertEquals(first, first);
    assertEquals(first, same);
    assertEquals(first.hashCode(), same.hashCode());
    assertNotEquals(first, otherDialect);
    assertNotEquals(first, null);
    assertNotEquals(first, "not a query");
  }

  @Test
  public void testFilterValidation() {
    assertThrows(
        IllegalArgumentException.class, () -> SemanticQueryFilter.of("", Operator.EQ, "1"));
    assertThrows(
        IllegalArgumentException.class, () -> SemanticQueryFilter.of(null, Operator.EQ, "1"));
    assertThrows(IllegalArgumentException.class, () -> SemanticQueryFilter.of("field", null, "1"));
    assertThrows(
        IllegalArgumentException.class, () -> SemanticQueryFilter.of("field", Operator.EQ, null));
  }

  @Test
  public void testFilterAccessorsAndEquality() {
    SemanticQueryFilter filter = SemanticQueryFilter.of("region", Operator.NE, "us-east");
    assertEquals("region", filter.field());
    assertEquals(Operator.NE, filter.operator());
    assertEquals("us-east", filter.value());

    assertEquals(filter, SemanticQueryFilter.of("region", Operator.NE, "us-east"));
    assertEquals(
        filter.hashCode(), SemanticQueryFilter.of("region", Operator.NE, "us-east").hashCode());
    assertNotEquals(filter, SemanticQueryFilter.of("region", Operator.EQ, "us-east"));
    assertNotEquals(filter, SemanticQueryFilter.of("area", Operator.NE, "us-east"));
    assertNotEquals(filter, SemanticQueryFilter.of("region", Operator.NE, "us-west"));
  }

  @Test
  public void testPlanValidation() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SemanticQueryPlan.of("", "trino", "planner", Collections.emptyList()));
    assertThrows(
        IllegalArgumentException.class,
        () -> SemanticQueryPlan.of("SELECT 1", " ", "planner", Collections.emptyList()));
    assertThrows(
        IllegalArgumentException.class,
        () -> SemanticQueryPlan.of("SELECT 1", "trino", null, Collections.emptyList()));
    assertThrows(
        IllegalArgumentException.class, () -> SemanticQueryPlan.of("SELECT 1", "trino", "p", null));
    assertThrows(
        IllegalArgumentException.class,
        () -> SemanticQueryPlan.of("SELECT 1", "trino", "p", Collections.singletonList(" ")));
  }

  @Test
  public void testPlanBuildAndEquality() {
    SemanticQueryPlan plan =
        SemanticQueryPlan.of("SELECT 1", "trino", "planner", Collections.singletonList("warning"));

    assertEquals("SELECT 1", plan.sql());
    assertEquals("trino", plan.dialect());
    assertEquals("planner", plan.planner());
    assertEquals(Collections.singletonList("warning"), plan.warnings());
    assertThrows(UnsupportedOperationException.class, () -> plan.warnings().add("other"));

    assertEquals(
        plan,
        SemanticQueryPlan.of("SELECT 1", "trino", "planner", Collections.singletonList("warning")));
    assertEquals(
        plan.hashCode(),
        SemanticQueryPlan.of("SELECT 1", "trino", "planner", Collections.singletonList("warning"))
            .hashCode());
    assertNotEquals(
        plan, SemanticQueryPlan.of("SELECT 2", "trino", "planner", Collections.singletonList("w")));
  }

  @Test
  public void testPlanningContext() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "model");

    PlanningContext empty = PlanningContext.of(ident);
    assertEquals(ident, empty.semanticModel());
    assertTrue(empty.properties().isEmpty());

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("timezone", "UTC");
    PlanningContext withProperties = PlanningContext.of(ident, properties);
    assertEquals(Collections.singletonMap("timezone", "UTC"), withProperties.properties());
    assertEquals(
        withProperties, PlanningContext.of(ident, Collections.singletonMap("timezone", "UTC")));
    assertEquals(
        withProperties.hashCode(),
        PlanningContext.of(ident, Collections.singletonMap("timezone", "UTC")).hashCode());
    assertNotEquals(empty, withProperties);
  }

  @Test
  public void testPlanningContextValidation() {
    assertThrows(IllegalArgumentException.class, () -> PlanningContext.of(null));
    assertThrows(
        IllegalArgumentException.class, () -> PlanningContext.of(NameIdentifier.of("model"), null));

    Map<String, String> nullValue = new LinkedHashMap<>();
    nullValue.put("key", null);
    assertThrows(
        IllegalArgumentException.class,
        () -> PlanningContext.of(NameIdentifier.of("model"), nullValue));

    Map<String, String> emptyKey = new LinkedHashMap<>();
    emptyKey.put("", "value");
    assertThrows(
        IllegalArgumentException.class,
        () -> PlanningContext.of(NameIdentifier.of("model"), emptyKey));
  }

  private static SemanticQuery query(String metric, String dialect) {
    return SemanticQuery.builder()
        .withMetrics(Collections.singletonList(metric))
        .withDimensions(Collections.emptyList())
        .withFilters(Collections.emptyList())
        .withDialect(dialect)
        .build();
  }
}
