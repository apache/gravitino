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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

  private static final NameIdentifier MODEL =
      NameIdentifier.of("metalake", "catalog", "schema", "model");
  private static final NameIdentifier ORDERS = NameIdentifier.of("catalog", "db", "orders");

  @Test
  public void testBuildQuery() {
    SemanticQueryFilter filter = SemanticQueryFilter.of("order_date", Operator.GE, "2024-01-01");
    SemanticQuery query =
        SemanticQuery.builder()
            .withMetrics(Arrays.asList("order_amount", "order_count"))
            .withDimensions(Collections.singletonList("region"))
            .withFilters(Collections.singletonList(filter))
            .withLimit(1000L)
            .build();

    assertEquals(Arrays.asList("order_amount", "order_count"), query.metrics());
    assertEquals(Collections.singletonList("region"), query.dimensions());
    assertEquals(Collections.singletonList(filter), query.filters());
    assertEquals(1000L, query.limit());
  }

  @Test
  public void testQueryWithoutLimit() {
    assertNull(query("metric", null).limit());
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
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticQuery.builder()
                .withMetrics(Collections.singletonList("metric"))
                .withDimensions(null)
                .withFilters(Collections.emptyList())
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticQuery.builder()
                .withMetrics(Collections.singletonList("metric"))
                .withDimensions(Collections.emptyList())
                .withFilters(null)
                .build());
  }

  @Test
  public void testQueryRejectsNegativeLimit() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticQuery.builder()
                .withMetrics(Collections.singletonList("metric"))
                .withDimensions(Collections.emptyList())
                .withFilters(Collections.emptyList())
                .withLimit(-1L)
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
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticQuery.builder()
                .withMetrics(Collections.singletonList("metric"))
                .withDimensions(Collections.singletonList(" "))
                .withFilters(Collections.emptyList())
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
                .build());
  }

  @Test
  public void testQueryIsImmutable() {
    SemanticQuery query =
        SemanticQuery.builder()
            .withMetrics(new ArrayList<>(Collections.singletonList("metric")))
            .withDimensions(Collections.emptyList())
            .withFilters(Collections.emptyList())
            .build();

    assertThrows(UnsupportedOperationException.class, () -> query.metrics().add("other"));
  }

  @Test
  public void testQueryEquality() {
    SemanticQuery first = query("metric", 10L);
    SemanticQuery same = query("metric", 10L);
    SemanticQuery otherLimit = query("metric", 20L);

    assertEquals(first, first);
    assertEquals(first, same);
    assertEquals(first.hashCode(), same.hashCode());
    assertNotEquals(first, otherLimit);
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
  public void testPlanBuildAndEquality() {
    SemanticQueryPlan plan = plan(1000L);

    assertEquals(MODEL, plan.semanticModel());
    assertEquals(3L, plan.semanticModelVersion());
    assertEquals(2, plan.sources().size());
    assertEquals("orders", plan.sources().get(0).dataset());
    assertEquals("regions", plan.sources().get(1).dataset());
    assertEquals(ORDERS, plan.sources().get(0).source());
    assertEquals(1, plan.joins().size());
    assertEquals(Collections.singletonList("Region"), plan.groupBy());
    assertEquals(1000L, plan.limit());
    assertEquals(Collections.singletonList("warning"), plan.warnings());
    assertThrows(UnsupportedOperationException.class, () -> plan.warnings().add("other"));
    assertThrows(UnsupportedOperationException.class, () -> plan.sources().add(null));

    assertEquals(plan, plan(1000L));
    assertEquals(plan.hashCode(), plan(1000L).hashCode());
    assertNotEquals(plan, plan(2000L));
  }

  @Test
  public void testPlanRequiresPositiveVersion() {
    assertThrows(
        IllegalArgumentException.class,
        () -> planBuilder(1000L).withSemanticModelVersion(0).build());
  }

  @Test
  public void testPlanValidatesSourcesAndSelections() {
    assertThrows(
        IllegalArgumentException.class,
        () -> planBuilder(1000L).withSources(Collections.emptyList()).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> planBuilder(1000L).withSelections(Collections.emptyList()).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> planBuilder(1000L).withGroupBy(Collections.singletonList("Missing")).build());
  }

  @Test
  public void testPlanValidatesJoinOrder() {
    PlanJoin reversed =
        PlanJoin.of(
            PlanJoin.Type.INNER,
            "orders_to_regions",
            "regions",
            new String[] {"id"},
            "orders",
            new String[] {"region_id"});
    assertThrows(
        IllegalArgumentException.class,
        () -> planBuilder(1000L).withJoins(Collections.singletonList(reversed)).build());
  }

  @Test
  public void testFieldReferenceRequiresExactlyOneRepresentation() {
    Expression expression = expression("orders.amount");
    assertThrows(
        IllegalArgumentException.class,
        () -> PlanFieldRef.ofExpression("orders", null, DataType.DECIMAL));
    assertThrows(IllegalArgumentException.class, () -> PlanFieldRef.ofColumn("orders", " ", null));
    assertEquals(
        expression, PlanFieldRef.ofExpression("orders", expression, DataType.DECIMAL).expression());
  }

  @Test
  public void testConversionContext() {
    ConversionContext empty = ConversionContext.of(Collections.emptyMap());
    assertEquals(Collections.emptyMap(), empty.properties());

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("catalog.orders", "hive.orders");
    ConversionContext context = ConversionContext.of(properties);
    assertEquals(Collections.singletonMap("catalog.orders", "hive.orders"), context.properties());
    assertThrows(UnsupportedOperationException.class, () -> context.properties().put("a", "b"));

    assertThrows(IllegalArgumentException.class, () -> ConversionContext.of(null));
    Map<String, String> nullValue = new LinkedHashMap<>();
    nullValue.put("key", null);
    assertThrows(IllegalArgumentException.class, () -> ConversionContext.of(nullValue));
    assertThrows(
        IllegalArgumentException.class,
        () -> ConversionContext.of(Collections.singletonMap("", "value")));
  }

  private static SemanticQuery query(String metric, Long limit) {
    return SemanticQuery.builder()
        .withMetrics(Collections.singletonList(metric))
        .withDimensions(Collections.emptyList())
        .withFilters(Collections.emptyList())
        .withLimit(limit)
        .build();
  }

  private static SemanticQueryPlan plan(Long limit) {
    return planBuilder(limit).build();
  }

  private static SemanticQueryPlan.Builder planBuilder(Long limit) {
    PlanJoin join =
        PlanJoin.of(
            PlanJoin.Type.INNER,
            "orders_to_regions",
            "orders",
            new String[] {"region_id"},
            "regions",
            new String[] {"id"});
    return SemanticQueryPlan.builder()
        .withSemanticModel(MODEL)
        .withSemanticModelVersion(3L)
        .withSources(
            Arrays.asList(PlanSource.of("orders", ORDERS), PlanSource.of("regions", ORDERS)))
        .withJoins(Collections.singletonList(join))
        .withSelections(
            Arrays.asList(
                PlanSelection.of(
                    "Gross_Revenue",
                    PlanSelection.Role.METRIC,
                    true,
                    PlanFieldRef.ofExpression(
                        "orders", expression("SUM(orders.amount)"), DataType.DECIMAL)),
                PlanSelection.of(
                    "Region",
                    PlanSelection.Role.DIMENSION,
                    false,
                    PlanFieldRef.ofExpression(
                        "regions", expression("regions.name"), DataType.STRING))))
        .withFilters(Collections.emptyList())
        .withGroupBy(Collections.singletonList("Region"))
        .withLimit(limit)
        .withWarnings(Collections.singletonList("warning"));
  }

  private static Expression expression(String text) {
    return Expression.builder()
        .withDialects(
            new DialectExpression[] {
              DialectExpression.builder()
                  .withDialect(Dialects.ANSI_SQL)
                  .withExpression(text)
                  .build()
            })
        .build();
  }
}
