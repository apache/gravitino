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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.Audit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.SemanticQueryPlanException;
import org.apache.gravitino.meta.AuditInfo;
import org.junit.jupiter.api.Test;

public class TestSemanticQueryResolver {

  private static final NameIdentifier IDENT =
      NameIdentifier.of("metalake", "catalog", "schema", "sales_model");
  private static final long VERSION = 7L;

  @Test
  public void testResolvesMetricDimensionJoinFilterAndLimit() {
    SemanticQuery query =
        SemanticQuery.builder()
            .withMetrics(Collections.singletonList("gross_revenue"))
            .withDimensions(Collections.singletonList("region_name"))
            .withFilters(
                Collections.singletonList(
                    SemanticQueryFilter.of(
                        "order_date", SemanticQueryFilter.Operator.GE, "2026-01-01")))
            .withLimit(100L)
            .build();

    SemanticQueryPlan plan = SemanticQueryResolver.resolve(IDENT, model(definition()), query);

    assertEquals(IDENT, plan.semanticModel());
    assertEquals(VERSION, plan.semanticModelVersion());
    assertEquals(100L, plan.limit());
    assertTrue(plan.warnings().isEmpty());
    assertEquals(Arrays.asList("orders", "regions"), datasets(plan.sources()));
    assertEquals(1, plan.joins().size());
    PlanJoin join = plan.joins().get(0);
    assertEquals(PlanJoin.Type.INNER, join.type());
    assertEquals("orders_to_regions", join.relationship());
    assertEquals("orders", join.fromDataset());
    assertEquals("regions", join.toDataset());
    assertEquals(Collections.singletonList("region_id"), Arrays.asList(join.fromColumns()));
    assertEquals(Collections.singletonList("region_id"), Arrays.asList(join.toColumns()));

    assertEquals(2, plan.selections().size());
    PlanSelection metric = plan.selections().get(0);
    assertEquals("gross_revenue", metric.name());
    assertEquals(PlanSelection.Role.METRIC, metric.role());
    assertTrue(metric.aggregate());
    assertEquals("orders", metric.field().dataset());
    assertEquals(DataType.DECIMAL, metric.field().datatype());
    assertEquals("SUM(orders.amount)", metric.field().expression().dialects()[0].expression());
    PlanSelection dimension = plan.selections().get(1);
    assertEquals("region_name", dimension.name());
    assertEquals(PlanSelection.Role.DIMENSION, dimension.role());
    assertFalse(dimension.aggregate());
    assertEquals("regions", dimension.field().dataset());
    assertEquals(Collections.singletonList("region_name"), plan.groupBy());

    assertEquals(1, plan.filters().size());
    PlanFilter filter = plan.filters().get(0);
    assertEquals("orders", filter.field().dataset());
    assertEquals(SemanticQueryFilter.Operator.GE, filter.operator());
    assertEquals("2026-01-01", filter.value());
  }

  @Test
  public void testMetricOwnershipFallsBackToDeclaredFields() {
    SemanticQuery query =
        SemanticQuery.builder()
            .withMetrics(Collections.singletonList("fallback_revenue"))
            .withDimensions(Collections.emptyList())
            .withFilters(Collections.emptyList())
            .build();

    SemanticQueryPlan plan = SemanticQueryResolver.resolve(IDENT, model(definition()), query);

    PlanSelection metric = plan.selections().get(0);
    assertEquals("fallback_revenue", metric.name());
    assertEquals("orders", metric.field().dataset());
    assertTrue(metric.aggregate());
    assertEquals(Collections.emptyList(), plan.groupBy());
    assertEquals(Collections.singletonList("orders"), datasets(plan.sources()));
  }

  @Test
  public void testAmbiguousMetricOwnershipRejected() {
    SemanticQuery query =
        query(Collections.singletonList("shared_metric"), Collections.emptyList());

    SemanticQueryPlanException exception =
        assertThrows(
            SemanticQueryPlanException.class,
            () -> SemanticQueryResolver.resolve(IDENT, model(definition()), query));
    assertTrue(exception.getMessage().contains("more than one dataset"));
  }

  @Test
  public void testMultiDatasetMetricRejected() {
    SemanticQuery query =
        query(Collections.singletonList("multi_dataset_metric"), Collections.emptyList());

    SemanticQueryPlanException exception =
        assertThrows(
            SemanticQueryPlanException.class,
            () -> SemanticQueryResolver.resolve(IDENT, model(definition()), query));
    assertTrue(exception.getMessage().contains("more than one dataset"));
  }

  @Test
  public void testUnattributableMetricRejected() {
    SemanticQuery query =
        query(Collections.singletonList("unowned_metric"), Collections.emptyList());

    SemanticQueryPlanException exception =
        assertThrows(
            SemanticQueryPlanException.class,
            () -> SemanticQueryResolver.resolve(IDENT, model(definition()), query));
    assertTrue(exception.getMessage().contains("cannot be attributed to a dataset"));
  }

  @Test
  public void testUnknownMetricRejected() {
    SemanticQuery query =
        query(Collections.singletonList("missing_metric"), Collections.emptyList());

    SemanticQueryPlanException exception =
        assertThrows(
            SemanticQueryPlanException.class,
            () -> SemanticQueryResolver.resolve(IDENT, model(definition()), query));
    assertTrue(exception.getMessage().contains("does not exist"));
  }

  @Test
  public void testAmbiguousBareFieldRejected() {
    SemanticQuery query =
        query(
            Collections.singletonList("gross_revenue"), Collections.singletonList("shared_field"));

    SemanticQueryPlanException exception =
        assertThrows(
            SemanticQueryPlanException.class,
            () -> SemanticQueryResolver.resolve(IDENT, model(definition()), query));
    assertTrue(exception.getMessage().contains("more than one dataset"));
  }

  @Test
  public void testUndeclaredSelectionBecomesPhysicalColumnOfFirstSelectionDataset() {
    SemanticQuery query =
        query(
            Collections.singletonList("gross_revenue"), Collections.singletonList("physical_only"));

    SemanticQueryPlan plan = SemanticQueryResolver.resolve(IDENT, model(definition()), query);

    PlanSelection dimension = plan.selections().get(1);
    assertEquals("physical_only", dimension.name());
    assertEquals("orders", dimension.field().dataset());
    assertEquals("physical_only", dimension.field().column());
  }

  @Test
  public void testUndeclaredFilterBecomesPhysicalColumnOfFirstSelectionDataset() {
    SemanticQuery query =
        SemanticQuery.builder()
            .withMetrics(Collections.singletonList("gross_revenue"))
            .withDimensions(Collections.emptyList())
            .withFilters(
                Collections.singletonList(
                    SemanticQueryFilter.of(
                        "physical_filter", SemanticQueryFilter.Operator.EQ, "value")))
            .build();

    SemanticQueryPlan plan = SemanticQueryResolver.resolve(IDENT, model(definition()), query);

    PlanFilter filter = plan.filters().get(0);
    assertEquals("orders", filter.field().dataset());
    assertEquals("physical_filter", filter.field().column());
    assertEquals(SemanticQueryFilter.Operator.EQ, filter.operator());
    assertEquals("value", filter.value());
  }

  @Test
  public void testUnreachableDatasetRejected() {
    SemanticQuery query =
        query(
            Collections.singletonList("gross_revenue"), Collections.singletonList("customer_name"));

    SemanticQueryPlanException exception =
        assertThrows(
            SemanticQueryPlanException.class,
            () -> SemanticQueryResolver.resolve(IDENT, model(definition()), query));
    assertTrue(exception.getMessage().contains("cannot be joined"));
  }

  @Test
  public void testShortestJoinPathIsUsed() {
    SemanticQuery query =
        query(
            Collections.singletonList("gross_revenue"),
            Arrays.asList("customer_name", "region_name"));

    SemanticQueryPlan plan =
        SemanticQueryResolver.resolve(IDENT, model(definitionWithCustomerLink()), query);

    assertEquals(Arrays.asList("orders", "regions", "customers"), datasets(plan.sources()));
    assertEquals(2, plan.joins().size());
    assertEquals("regions", plan.joins().get(0).toDataset());
    assertEquals("customers", plan.joins().get(1).toDataset());
    assertEquals("regions", plan.joins().get(1).fromDataset());
    assertEquals("regions_to_customers", plan.joins().get(1).relationship());
  }

  @Test
  public void testDuplicateSelectionRejected() {
    SemanticQuery query =
        query(
            Collections.singletonList("gross_revenue"), Collections.singletonList("gross_revenue"));

    assertThrows(
        SemanticQueryPlanException.class,
        () -> SemanticQueryResolver.resolve(IDENT, model(definition()), query));
  }

  @Test
  public void testPlanIsDeterministic() {
    SemanticQuery query =
        query(Collections.singletonList("gross_revenue"), Collections.singletonList("region_name"));

    SemanticQueryPlan first = SemanticQueryResolver.resolve(IDENT, model(definition()), query);
    SemanticQueryPlan second = SemanticQueryResolver.resolve(IDENT, model(definition()), query);

    assertEquals(first, second);
  }

  @Test
  public void testModelWithoutCommittedVersionRejected() {
    SemanticQuery query =
        query(Collections.singletonList("gross_revenue"), Collections.emptyList());

    assertThrows(
        IllegalStateException.class,
        () -> SemanticQueryResolver.resolve(IDENT, model(definition(), 0L), query));
  }

  private static SemanticQuery query(List<String> metrics, List<String> dimensions) {
    return SemanticQuery.builder()
        .withMetrics(metrics)
        .withDimensions(dimensions)
        .withFilters(Collections.emptyList())
        .build();
  }

  private static List<String> datasets(List<PlanSource> sources) {
    return sources.stream().map(PlanSource::dataset).collect(Collectors.toList());
  }

  private static SemanticModel model(SemanticModelDefinition definition) {
    return model(definition, VERSION);
  }

  private static SemanticModel model(SemanticModelDefinition definition, long version) {
    return new TestSemanticModel(definition, version);
  }

  private static SemanticModelDefinition definition() {
    return SemanticModelDefinition.builder()
        .withDatasets(
            new Dataset[] {
              dataset("orders", "order_id", "amount", "region_id", "order_date", "shared_field"),
              dataset("regions", "region_id", "region_name", "shared_field"),
              dataset("customers", "customer_id", "customer_name")
            })
        .withRelationships(
            new Relationship[] {
              Relationship.builder()
                  .withName("orders_to_regions")
                  .withFrom("orders")
                  .withTo("regions")
                  .withFromColumns(new String[] {"region_id"})
                  .withToColumns(new String[] {"region_id"})
                  .build()
            })
        .withMetrics(
            new Metric[] {
              metric("gross_revenue", "SUM(orders.amount)", DataType.DECIMAL),
              metric("fallback_revenue", "SUM(amount)", DataType.DECIMAL),
              metric("shared_metric", "SUM(shared_field)", DataType.DECIMAL),
              metric("multi_dataset_metric", "orders.amount + regions.region_id", DataType.DECIMAL),
              metric("unowned_metric", "SUM(1)", DataType.DECIMAL)
            })
        .build();
  }

  private static SemanticModelDefinition definitionWithCustomerLink() {
    return SemanticModelDefinition.builder()
        .withDatasets(
            new Dataset[] {
              dataset("orders", "order_id", "amount", "region_id", "order_date"),
              dataset("regions", "region_id", "region_name"),
              dataset("customers", "customer_id", "customer_name")
            })
        .withRelationships(
            new Relationship[] {
              Relationship.builder()
                  .withName("orders_to_regions")
                  .withFrom("orders")
                  .withTo("regions")
                  .withFromColumns(new String[] {"region_id"})
                  .withToColumns(new String[] {"region_id"})
                  .build(),
              Relationship.builder()
                  .withName("regions_to_customers")
                  .withFrom("regions")
                  .withTo("customers")
                  .withFromColumns(new String[] {"region_id"})
                  .withToColumns(new String[] {"customer_id"})
                  .build()
            })
        .withMetrics(new Metric[] {metric("gross_revenue", "SUM(orders.amount)", DataType.DECIMAL)})
        .build();
  }

  private static Dataset dataset(String name, String... fieldNames) {
    Field[] fields =
        Arrays.stream(fieldNames)
            .map(
                fieldName ->
                    Field.builder()
                        .withName(fieldName)
                        .withExpression(expression(fieldName))
                        .withDatatype(DataType.STRING)
                        .build())
            .toArray(Field[]::new);
    return Dataset.builder()
        .withName(name)
        .withSource(NameIdentifier.of("catalog", "schema", name + "_table"))
        .withPrimaryKey(new String[] {fieldNames[0]})
        .withFields(fields)
        .build();
  }

  private static Metric metric(String name, String text, DataType datatype) {
    return Metric.builder()
        .withName(name)
        .withExpression(expression(text))
        .withDatatype(datatype)
        .build();
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

  private static final class TestSemanticModel implements SemanticModel {

    private final SemanticModelDefinition definition;
    private final long version;

    private TestSemanticModel(SemanticModelDefinition definition, long version) {
      this.definition = definition;
      this.version = version;
    }

    @Override
    public String name() {
      return IDENT.name();
    }

    @Override
    public String comment() {
      return null;
    }

    @Override
    public long version() {
      return version;
    }

    @Override
    public SemanticModelDefinition definition() {
      return definition;
    }

    @Override
    public Audit auditInfo() {
      return AuditInfo.builder()
          .withCreator("tester")
          .withCreateTime(Instant.parse("2026-08-25T00:00:00Z"))
          .build();
    }
  }
}
