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
package org.apache.gravitino.catalog;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Dialects;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Field;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.junit.jupiter.api.Test;

public class TestSemanticModelValidator {

  @Test
  public void testValidCompleteDefinitionWithoutExternalResolution() {
    CustomExtension extension = extension();
    Dataset orders =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("missing_catalog", "missing_schema", "orders"))
            .withPrimaryKey(new String[] {"order_id"})
            .withUniqueKeys(new String[][] {{"order_id"}, {"external_id", "source"}})
            .withFields(
                new Field[] {
                  field("id", expression("order_id")), field("amount", expression("amount"))
                })
            .withCustomExtensions(new CustomExtension[] {extension})
            .build();
    Dataset customers =
        Dataset.builder()
            .withName("customers")
            .withSource(NameIdentifier.of("missing_catalog", "missing_schema", "customers"))
            .withFields(new Field[] {field("id", expression("customer_id"))})
            .build();
    Relationship relationship =
        relationship(
            "orders_to_customers",
            "orders",
            "customers",
            new String[] {"customer_id", "tenant_id"},
            new String[] {"id", "tenant_id"});
    Metric metric =
        Metric.builder()
            .withName("total_revenue")
            .withExpression(
                multiDialectExpression(
                    "SUM(orders.amount) /* text is intentionally uninterpreted */",
                    "SUM(orders.amount)"))
            .withCustomExtensions(new CustomExtension[] {extension})
            .build();

    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {orders, customers})
            .withRelationships(new Relationship[] {relationship})
            .withMetrics(new Metric[] {metric})
            .withCustomExtensions(new CustomExtension[] {extension})
            .build();

    assertDoesNotThrow(() -> SemanticModelValidator.validateDefinition(definition));
  }

  @Test
  public void testDefinitionAndDatasetConstraints() {
    assertInvalid(null, "$: definition must not be null");

    assertInvalid(
        definition(dataset("orders"), dataset("orders")),
        "datasets[1].name: duplicate dataset name 'orders'; first declared at datasets[0].name");

    Dataset shortSource =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "orders"))
            .build();
    assertInvalid(
        definition(shortSource),
        "datasets[0].source: must contain exactly catalog.schema.name, but was 'sales.orders'");
  }

  @Test
  public void testFieldNamesAreUniquePerDataset() {
    Dataset duplicateFields =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withFields(
                new Field[] {field("id", expression("id")), field("id", expression("order_id"))})
            .build();

    assertInvalid(
        definition(duplicateFields),
        "datasets[0].fields[1].name: duplicate field name 'id'; first declared at "
            + "datasets[0].fields[0].name");

    assertDoesNotThrow(
        () ->
            SemanticModelValidator.validateDefinition(
                definition(datasetWithField("orders", "id"), datasetWithField("customers", "id"))));
  }

  @Test
  public void testRelationshipNamesAndEndpoints() {
    Dataset orders = dataset("orders");
    Dataset customers = dataset("customers");
    Relationship relationship = relationship("by_customer", "orders", "customers");

    assertInvalid(
        definition(
            new Dataset[] {orders, customers},
            new Relationship[] {relationship, relationship},
            null),
        "relationships[1].name: duplicate relationship name 'by_customer'; first declared at "
            + "relationships[0].name");
    assertInvalid(
        definition(
            new Dataset[] {orders},
            new Relationship[] {relationship("missing", "orders", "customers")},
            null),
        "relationships[0].to: unknown dataset 'customers'; relationship endpoints must reference "
            + "datasets in the same model");
  }

  @Test
  public void testMetricNames() {
    Metric revenue = metric("revenue", expression("SUM(amount)"));
    assertInvalid(
        definition(new Dataset[] {dataset("orders")}, null, new Metric[] {revenue, revenue}),
        "metrics[1].name: duplicate metric name 'revenue'; first declared at metrics[0].name");
  }

  private static void assertInvalid(
      @Nullable SemanticModelDefinition definition, String expectedMessage) {
    IllegalSemanticModelException exception =
        assertThrows(
            IllegalSemanticModelException.class,
            () -> SemanticModelValidator.validateDefinition(definition));
    assertEquals(expectedMessage, exception.getMessage());
  }

  private static SemanticModelDefinition definition(Dataset... datasets) {
    return SemanticModelDefinition.builder().withDatasets(datasets).build();
  }

  private static SemanticModelDefinition definition(
      Dataset[] datasets, @Nullable Relationship[] relationships, @Nullable Metric[] metrics) {
    return SemanticModelDefinition.builder()
        .withDatasets(datasets)
        .withRelationships(relationships)
        .withMetrics(metrics)
        .build();
  }

  private static Dataset dataset(String name) {
    return Dataset.builder()
        .withName(name)
        .withSource(NameIdentifier.of("sales", "mart", name))
        .build();
  }

  private static Dataset datasetWithField(String name, String fieldName) {
    return Dataset.builder()
        .withName(name)
        .withSource(NameIdentifier.of("sales", "mart", name))
        .withFields(new Field[] {field(fieldName, expression(fieldName))})
        .build();
  }

  private static Field field(String name, Expression expression) {
    return Field.builder().withName(name).withExpression(expression).build();
  }

  private static Metric metric(String name, Expression expression) {
    return Metric.builder().withName(name).withExpression(expression).build();
  }

  private static Relationship relationship(String name, String from, String to) {
    return relationship(name, from, to, new String[] {"customer_id"}, new String[] {"id"});
  }

  private static Relationship relationship(
      String name, String from, String to, String[] fromColumns, String[] toColumns) {
    return Relationship.builder()
        .withName(name)
        .withFrom(from)
        .withTo(to)
        .withFromColumns(fromColumns)
        .withToColumns(toColumns)
        .build();
  }

  private static Expression expression(String value) {
    return Expression.builder()
        .withDialects(new DialectExpression[] {dialect(Dialects.ANSI_SQL, value)})
        .build();
  }

  private static Expression multiDialectExpression(String ansi, String bigQuery) {
    return Expression.builder()
        .withDialects(
            new DialectExpression[] {
              dialect(Dialects.ANSI_SQL, ansi), dialect(Dialects.BIGQUERY, bigQuery)
            })
        .build();
  }

  private static DialectExpression dialect(String dialect, String expression) {
    return DialectExpression.builder().withDialect(dialect).withExpression(expression).build();
  }

  private static CustomExtension extension() {
    return CustomExtension.builder().withVendorName("example").withData("{}").build();
  }
}
