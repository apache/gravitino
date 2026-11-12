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

import static org.apache.gravitino.semantic.CustomExtension.GRAVITINO_PROPERTIES_VENDOR;
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
                  field("order_id", expression("o_orderkey")),
                  field("external_id", expression("external_id")),
                  field("source", expression("source")),
                  field("customer_id", expression("o_custkey")),
                  field("tenant_id", expression("tenant_id")),
                  field("amount", expression("amount"))
                })
            .withCustomExtensions(new CustomExtension[] {extension})
            .build();
    Dataset customers =
        Dataset.builder()
            .withName("customers")
            .withSource(NameIdentifier.of("missing_catalog", "missing_schema", "customers"))
            .withFields(
                new Field[] {
                  field("id", expression("customer_id")),
                  field("tenant_id", expression("tenant_id"))
                })
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
  public void testRejectsReservedRootExtensionRegardlessOfData() {
    for (String data : new String[] {"{}", "{\"owner\":\"analytics\"}", "not JSON"}) {
      CustomExtension reserved =
          CustomExtension.builder()
              .withVendorName(GRAVITINO_PROPERTIES_VENDOR)
              .withData(data)
              .build();
      SemanticModelDefinition definition =
          SemanticModelDefinition.builder()
              .withDatasets(new Dataset[] {dataset("orders")})
              .withCustomExtensions(new CustomExtension[] {reserved})
              .build();

      assertInvalid(
          definition,
          "customExtensions[0].vendorName: 'GRAVITINO_PROPERTIES' is reserved for Gravitino properties");
    }
  }

  @Test
  public void testPreservesOrdinaryRootAndNestedPropertiesVendorExtensions() {
    CustomExtension nested =
        CustomExtension.builder()
            .withVendorName(GRAVITINO_PROPERTIES_VENDOR)
            .withData("not JSON")
            .build();
    Dataset orders =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withCustomExtensions(new CustomExtension[] {nested})
            .build();
    CustomExtension ordinary =
        CustomExtension.builder().withVendorName("GRAVITINO").withData("not JSON").build();
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {orders})
            .withCustomExtensions(new CustomExtension[] {ordinary})
            .build();

    assertDoesNotThrow(() -> SemanticModelValidator.validateDefinition(definition));
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
    Dataset orders = datasetWithField("orders", "customer_id");
    Dataset customers = datasetWithField("customers", "id");
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

  @Test
  public void testKeyReferencesResolveToDeclaredFields() {
    Dataset aliased =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withPrimaryKey(new String[] {"Order_ID"})
            .withUniqueKeys(new String[][] {{"order_id", "Tenant_ID"}})
            .withFields(
                new Field[] {
                  field("order_id", expression("o_orderkey")),
                  field("tenant_id", expression("o_tenantkey"))
                })
            .build();
    assertDoesNotThrow(() -> SemanticModelValidator.validateDefinition(definition(aliased)));

    // A source column name is not a field reference even when a field expression uses it.
    Dataset physicalColumn =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withPrimaryKey(new String[] {"o_orderkey"})
            .withFields(new Field[] {field("order_id", expression("o_orderkey"))})
            .build();
    assertInvalid(
        definition(physicalColumn),
        "datasets[0].primaryKey[0]: 'o_orderkey' is not a declared field of dataset 'orders'");

    Dataset uniqueKeyViolation =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withUniqueKeys(new String[][] {{"order_id"}, {"region", "tenant_id"}})
            .withFields(
                new Field[] {
                  field("order_id", expression("o_orderkey")),
                  field("tenant_id", expression("o_tenantkey"))
                })
            .build();
    assertInvalid(
        definition(uniqueKeyViolation),
        "datasets[0].uniqueKeys[1][0]: 'region' is not a declared field of dataset 'orders'");

    Dataset noFields =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withPrimaryKey(new String[] {"order_id"})
            .build();
    assertInvalid(
        definition(noFields),
        "datasets[0].primaryKey[0]: 'order_id' is not a declared field of dataset 'orders'");
  }

  @Test
  public void testRelationshipColumnsResolveToEndpointFields() {
    Dataset orders = datasetWithFields("orders", "customer_id", "tenant_id");
    Dataset customers = datasetWithFields("customers", "id", "tenant_id");

    assertDoesNotThrow(
        () ->
            SemanticModelValidator.validateDefinition(
                definition(
                    new Dataset[] {orders, customers},
                    new Relationship[] {
                      relationship(
                          "by_customer",
                          "orders",
                          "customers",
                          new String[] {"Customer_ID", "TENANT_ID"},
                          new String[] {"id", "Tenant_Id"})
                    },
                    null)));

    assertInvalid(
        definition(
            new Dataset[] {orders, customers},
            new Relationship[] {
              relationship(
                  "bad_from", "orders", "customers", new String[] {"missing"}, new String[] {"id"})
            },
            null),
        "relationships[0].fromColumns[0]: 'missing' is not a declared field of dataset 'orders'");

    assertInvalid(
        definition(
            new Dataset[] {orders, customers},
            new Relationship[] {
              relationship(
                  "bad_to",
                  "orders",
                  "customers",
                  new String[] {"customer_id"},
                  new String[] {"missing"})
            },
            null),
        "relationships[0].toColumns[0]: 'missing' is not a declared field of dataset 'customers'");

    // A dataset that declares no fields exposes nothing to relationship columns.
    Dataset fieldless = dataset("customers");
    assertInvalid(
        definition(
            new Dataset[] {orders, fieldless},
            new Relationship[] {
              relationship(
                  "bad_to",
                  "orders",
                  "customers",
                  new String[] {"customer_id"},
                  new String[] {"id"})
            },
            null),
        "relationships[0].toColumns[0]: 'id' is not a declared field of dataset 'customers'");
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
    return datasetWithFields(name, fieldName);
  }

  private static Dataset datasetWithFields(String name, String... fieldNames) {
    Field[] fields = new Field[fieldNames.length];
    for (int index = 0; index < fieldNames.length; index++) {
      fields[index] = field(fieldNames[index], expression(fieldNames[index]));
    }
    return Dataset.builder()
        .withName(name)
        .withSource(NameIdentifier.of("sales", "mart", name))
        .withFields(fields)
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
