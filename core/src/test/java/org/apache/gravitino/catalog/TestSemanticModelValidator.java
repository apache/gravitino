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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.AIContextObject;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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

  @ParameterizedTest
  @MethodSource("validAIContexts")
  public void testAIContextAcceptsTextAndObjectVariants(AIContext aiContext) {
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withAIContext(aiContext)
            .withDatasets(new Dataset[] {dataset("orders")})
            .build();

    assertDoesNotThrow(() -> SemanticModelValidator.validateDefinition(definition));
  }

  @Test
  public void testDefinitionValidationAcceptsCustomDialectsWithoutParsingSql() {
    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withFields(
                new Field[] {
                  field(
                      "order_id",
                      Expression.builder()
                          .withDialects(
                              new DialectExpression[] {
                                dialect("TRINO", "not parsed by metadata validation"),
                                dialect("trino", "also preserved exactly")
                              })
                          .build())
                })
            .build();

    assertDoesNotThrow(() -> SemanticModelValidator.validateDefinition(definition(dataset)));
  }

  @Test
  public void testValidateForWriteComposesDefinitionAndSourceValidation() throws Exception {
    CatalogManager catalogManager = mock(CatalogManager.class);
    TableDispatcher tableDispatcher = mock(TableDispatcher.class);
    ViewDispatcher viewDispatcher = mock(ViewDispatcher.class);
    CatalogManager.CatalogWrapper wrapper = mock(CatalogManager.CatalogWrapper.class);
    when(wrapper.capabilities()).thenReturn(Capability.DEFAULT);
    when(catalogManager.loadCatalogAndWrap(NameIdentifier.of("metalake", "sales")))
        .thenReturn(wrapper);

    Column column = mock(Column.class);
    when(column.name()).thenReturn("order_id");
    Table table = mock(Table.class);
    when(table.columns()).thenReturn(new Column[] {column});
    NameIdentifier source = NameIdentifier.of(Namespace.of("metalake", "sales", "mart"), "orders");
    when(tableDispatcher.loadTable(source)).thenReturn(table);

    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withPrimaryKey(new String[] {"order_id"})
            .build();
    NameIdentifier model =
        NameIdentifier.of(Namespace.of("metalake", "metadata", "models"), "sales_model");
    SemanticModelValidator validator =
        new SemanticModelValidator(catalogManager, tableDispatcher, viewDispatcher);

    assertDoesNotThrow(() -> validator.validateForWrite(model, definition(dataset)));
    verify(tableDispatcher).loadTable(source);
    verifyNoInteractions(viewDispatcher);
  }

  @Test
  public void testDefinitionFailurePreventsSourceResolution() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    TableDispatcher tableDispatcher = mock(TableDispatcher.class);
    ViewDispatcher viewDispatcher = mock(ViewDispatcher.class);
    SemanticModelValidator validator =
        new SemanticModelValidator(catalogManager, tableDispatcher, viewDispatcher);
    NameIdentifier model =
        NameIdentifier.of(Namespace.of("metalake", "metadata", "models"), "sales_model");

    assertThrows(
        IllegalSemanticModelException.class,
        () -> validator.validateForWrite(model, definition(dataset("orders"), dataset("orders"))));
    verifyNoInteractions(catalogManager, tableDispatcher, viewDispatcher);
  }

  @Test
  public void testOptionalArraysDistinguishAbsentAndExplicitEmpty() {
    SemanticModelDefinition absent =
        SemanticModelDefinition.builder().withDatasets(new Dataset[] {dataset("orders")}).build();
    Dataset explicitEmptyDataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withPrimaryKey(new String[0])
            .withUniqueKeys(new String[0][])
            .withFields(new Field[0])
            .withCustomExtensions(new CustomExtension[0])
            .build();
    SemanticModelDefinition explicitEmpty =
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {explicitEmptyDataset})
            .withRelationships(new Relationship[0])
            .withMetrics(new Metric[0])
            .withCustomExtensions(new CustomExtension[0])
            .build();

    assertDoesNotThrow(() -> SemanticModelValidator.validateDefinition(absent));
    assertDoesNotThrow(() -> SemanticModelValidator.validateDefinition(explicitEmpty));
  }

  @Test
  public void testDefinitionAndDatasetStructure() {
    assertInvalid(null, "$: definition must not be null");

    SemanticModelDefinition nullDataset = mock(SemanticModelDefinition.class);
    when(nullDataset.datasets()).thenReturn(new Dataset[] {null});
    assertInvalid(nullDataset, "datasets[0]: must not be null");

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
  public void testAIContextMustContainExactlyOneTypedVariant() {
    AIContext invalidContext = mock(AIContext.class);
    when(invalidContext.text()).thenReturn(null);
    when(invalidContext.object()).thenReturn(null);
    SemanticModelDefinition invalidDefinition = mock(SemanticModelDefinition.class);
    when(invalidDefinition.aiContext()).thenReturn(invalidContext);
    when(invalidDefinition.datasets()).thenReturn(new Dataset[] {dataset("orders")});

    assertInvalid(invalidDefinition, "aiContext: must contain exactly one string or object value");

    AIContext bothVariants = mock(AIContext.class);
    when(bothVariants.text()).thenReturn("Use certified definitions");
    when(bothVariants.object()).thenReturn(AIContextObject.builder().build());
    when(invalidDefinition.aiContext()).thenReturn(bothVariants);
    assertInvalid(invalidDefinition, "aiContext: must contain exactly one string or object value");
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
  public void testDatasetKeyShapesAreDefensivelyValidated() {
    Dataset invalidPrimary = mockDataset("orders");
    when(invalidPrimary.primaryKey()).thenReturn(new String[] {null});
    assertInvalid(
        definition(invalidPrimary), "datasets[0].primaryKey[0]: must not be null or empty");

    Dataset invalidUnique = mockDataset("orders");
    when(invalidUnique.uniqueKeys()).thenReturn(new String[][] {new String[0]});
    assertInvalid(
        definition(invalidUnique), "datasets[0].uniqueKeys[0]: must not be null or empty");
  }

  @Test
  public void testRelationshipNamesEndpointsAndColumns() {
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

    Relationship mismatched = mock(Relationship.class);
    when(mismatched.name()).thenReturn("mismatched");
    when(mismatched.from()).thenReturn("orders");
    when(mismatched.to()).thenReturn("customers");
    when(mismatched.fromColumns()).thenReturn(new String[] {"customer_id", "tenant_id"});
    when(mismatched.toColumns()).thenReturn(new String[] {"id"});
    assertInvalid(
        definition(new Dataset[] {orders, customers}, new Relationship[] {mismatched}, null),
        "relationships[0].toColumns: must contain 2 columns to match "
            + "relationships[0].fromColumns, but contained 1");
  }

  @Test
  public void testMetricNamesAndExpressions() {
    Metric revenue = metric("revenue", expression("SUM(amount)"));
    assertInvalid(
        definition(new Dataset[] {dataset("orders")}, null, new Metric[] {revenue, revenue}),
        "metrics[1].name: duplicate metric name 'revenue'; first declared at metrics[0].name");

    DialectExpression ansi = dialect(Dialects.ANSI_SQL, "SUM(amount)");
    Expression duplicateDialect = mock(Expression.class);
    when(duplicateDialect.dialects()).thenReturn(new DialectExpression[] {ansi, ansi});
    assertInvalid(
        definition(
            new Dataset[] {dataset("orders")},
            null,
            new Metric[] {metric("revenue", duplicateDialect)}),
        "metrics[0].expression.dialects[1].dialect: duplicate dialect 'ANSI_SQL'; first declared "
            + "at metrics[0].expression.dialects[0].dialect");
  }

  @Test
  public void testNullNestedMembersAreDefensivelyValidated() {
    Dataset nullField = mockDataset("orders");
    when(nullField.fields()).thenReturn(new Field[] {null});
    assertInvalid(definition(nullField), "datasets[0].fields[0]: must not be null");

    SemanticModelDefinition nullRelationship = mock(SemanticModelDefinition.class);
    when(nullRelationship.datasets()).thenReturn(new Dataset[] {dataset("orders")});
    when(nullRelationship.relationships()).thenReturn(new Relationship[] {null});
    assertInvalid(nullRelationship, "relationships[0]: must not be null");

    SemanticModelDefinition nullMetric = mock(SemanticModelDefinition.class);
    when(nullMetric.datasets()).thenReturn(new Dataset[] {dataset("orders")});
    when(nullMetric.metrics()).thenReturn(new Metric[] {null});
    assertInvalid(nullMetric, "metrics[0]: must not be null");
  }

  private static void assertInvalid(
      @Nullable SemanticModelDefinition definition, String expectedMessage) {
    IllegalSemanticModelException exception =
        assertThrows(
            IllegalSemanticModelException.class,
            () -> SemanticModelValidator.validateDefinition(definition));
    assertEquals(expectedMessage, exception.getMessage());
  }

  private static Stream<AIContext> validAIContexts() {
    return Stream.of(
        AIContext.of("Use certified definitions"),
        AIContext.of(
            AIContextObject.builder()
                .withInstructions("Use certified definitions")
                .withSynonyms(new String[] {"sales", "bookings"})
                .withExamples(new String[] {"Revenue by day"})
                .build()));
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

  private static Dataset mockDataset(String name) {
    Dataset dataset = mock(Dataset.class);
    when(dataset.name()).thenReturn(name);
    when(dataset.source()).thenReturn(NameIdentifier.of("sales", "mart", name));
    return dataset;
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
