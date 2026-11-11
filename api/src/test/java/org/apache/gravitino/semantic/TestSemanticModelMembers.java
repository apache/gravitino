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

import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Test;

public class TestSemanticModelMembers {

  @Test
  public void testBuildMembers() {
    Expression expression = expression("order_amount");
    AIContext aiContext = AIContext.of("Use certified fields");
    CustomExtension extension =
        CustomExtension.builder().withVendorName("example").withData("{\"key\":\"value\"}").build();
    Field field =
        Field.builder()
            .withName("order_amount")
            .withExpression(expression)
            .withDimension(Dimension.builder().withIsTime(false).build())
            .withLabel("Order amount")
            .withDescription("Gross order amount")
            .withDatatype(DataType.DECIMAL)
            .withAIContext(aiContext)
            .withCustomExtensions(new CustomExtension[] {extension})
            .build();
    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withPrimaryKey(new String[] {"order_id"})
            .withUniqueKeys(new String[][] {{"order_id"}})
            .withDescription("Orders")
            .withAIContext(aiContext)
            .withFields(new Field[] {field})
            .withCustomExtensions(new CustomExtension[] {extension})
            .build();
    Relationship relationship =
        Relationship.builder()
            .withName("orders_to_customers")
            .withFrom("orders")
            .withTo("customers")
            .withFromColumns(new String[] {"customer_id"})
            .withToColumns(new String[] {"id"})
            .withAIContext(aiContext)
            .build();
    Metric metric =
        Metric.builder()
            .withName("total_revenue")
            .withExpression(expression)
            .withDescription("Total revenue")
            .withDatatype(DataType.DECIMAL)
            .withAIContext(aiContext)
            .build();

    assertEquals("orders", dataset.name());
    assertEquals(NameIdentifier.of("sales", "mart", "orders"), dataset.source());
    assertEquals("order_amount", dataset.fields()[0].name());
    assertEquals("orders", relationship.from());
    assertEquals("customers", relationship.to());
    assertEquals(DataType.DECIMAL, metric.datatype());
  }

  @Test
  public void testCollectionsAreDefensivelyCopied() {
    Expression expression = expression("order_id");
    String[] primaryKey = {"order_id"};
    String[][] uniqueKeys = {{"order_id"}};
    Field[] fields = {Field.builder().withName("order_id").withExpression(expression).build()};
    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withPrimaryKey(primaryKey)
            .withUniqueKeys(uniqueKeys)
            .withFields(fields)
            .build();
    String[] fromColumns = {"customer_id"};
    String[] toColumns = {"id"};
    Relationship relationship =
        Relationship.builder()
            .withName("orders_to_customers")
            .withFrom("orders")
            .withTo("customers")
            .withFromColumns(fromColumns)
            .withToColumns(toColumns)
            .build();

    primaryKey[0] = "changed";
    uniqueKeys[0][0] = "changed";
    fields[0] = Field.builder().withName("changed").withExpression(expression).build();
    fromColumns[0] = "changed";
    toColumns[0] = "changed";

    assertArrayEquals(new String[] {"order_id"}, dataset.primaryKey());
    assertArrayEquals(new String[] {"order_id"}, dataset.uniqueKeys()[0]);
    assertEquals("order_id", dataset.fields()[0].name());
    assertArrayEquals(new String[] {"customer_id"}, relationship.fromColumns());
    assertArrayEquals(new String[] {"id"}, relationship.toColumns());
    assertNotSame(dataset.uniqueKeys(), dataset.uniqueKeys());
    assertNotSame(dataset.uniqueKeys()[0], dataset.uniqueKeys()[0]);
  }

  @Test
  public void testOptionalCollectionsPreserveNullAndEmpty() {
    Expression expression = expression("id");
    Field unsetField = Field.builder().withName("id").withExpression(expression).build();
    Field emptyField =
        Field.builder()
            .withName("id")
            .withExpression(expression)
            .withCustomExtensions(new CustomExtension[0])
            .build();
    Dataset unsetDataset = datasetBuilder().build();
    Dataset emptyDataset =
        datasetBuilder()
            .withPrimaryKey(new String[0])
            .withUniqueKeys(new String[0][])
            .withFields(new Field[0])
            .withCustomExtensions(new CustomExtension[0])
            .build();
    Relationship unsetRelationship = relationshipBuilder().build();
    Relationship emptyRelationship =
        relationshipBuilder().withCustomExtensions(new CustomExtension[0]).build();
    Metric unsetMetric = Metric.builder().withName("count").withExpression(expression).build();
    Metric emptyMetric =
        Metric.builder()
            .withName("count")
            .withExpression(expression)
            .withCustomExtensions(new CustomExtension[0])
            .build();

    assertNull(unsetDataset.primaryKey());
    assertNull(unsetDataset.uniqueKeys());
    assertNull(unsetDataset.fields());
    assertNull(unsetDataset.customExtensions());
    assertNull(unsetField.customExtensions());
    assertNull(unsetRelationship.customExtensions());
    assertNull(unsetMetric.customExtensions());

    assertArrayEquals(new String[0], emptyDataset.primaryKey());
    assertEquals(0, emptyDataset.uniqueKeys().length);
    assertArrayEquals(new Field[0], emptyDataset.fields());
    assertArrayEquals(new CustomExtension[0], emptyDataset.customExtensions());
    assertArrayEquals(new CustomExtension[0], emptyField.customExtensions());
    assertArrayEquals(new CustomExtension[0], emptyRelationship.customExtensions());
    assertArrayEquals(new CustomExtension[0], emptyMetric.customExtensions());

    assertNotEquals(unsetDataset, emptyDataset);
    assertNotEquals(unsetField, emptyField);
    assertNotEquals(unsetRelationship, emptyRelationship);
    assertNotEquals(unsetMetric, emptyMetric);
  }

  @Test
  public void testRequiredBuilderFields() {
    assertThrows(IllegalArgumentException.class, () -> Field.builder().build());
    assertThrows(IllegalArgumentException.class, () -> Dataset.builder().build());
    assertThrows(IllegalArgumentException.class, () -> Relationship.builder().build());
    assertThrows(IllegalArgumentException.class, () -> Metric.builder().build());
  }

  @Test
  public void testArrayElementValidation() {
    Expression expression = expression("id");

    assertThrows(
        IllegalArgumentException.class,
        () -> datasetBuilder().withPrimaryKey(new String[] {null}).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> datasetBuilder().withPrimaryKey(new String[] {""}).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> datasetBuilder().withUniqueKeys(new String[][] {null}).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> datasetBuilder().withUniqueKeys(new String[][] {new String[0]}).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> datasetBuilder().withUniqueKeys(new String[][] {{null}}).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> datasetBuilder().withFields(new Field[] {null}).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> datasetBuilder().withCustomExtensions(new CustomExtension[] {null}).build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            Field.builder()
                .withName("id")
                .withExpression(expression)
                .withCustomExtensions(new CustomExtension[] {null})
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            Metric.builder()
                .withName("count")
                .withExpression(expression)
                .withCustomExtensions(new CustomExtension[] {null})
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () -> relationshipBuilder().withFromColumns(new String[] {null}).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> relationshipBuilder().withToColumns(new String[] {""}).build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            relationshipBuilder()
                .withFromColumns(new String[] {"tenant_id", "customer_id"})
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () -> relationshipBuilder().withCustomExtensions(new CustomExtension[] {null}).build());
  }

  private static Dataset.Builder datasetBuilder() {
    return Dataset.builder()
        .withName("orders")
        .withSource(NameIdentifier.of("sales", "mart", "orders"));
  }

  private static Relationship.Builder relationshipBuilder() {
    return Relationship.builder()
        .withName("orders_to_customers")
        .withFrom("orders")
        .withTo("customers")
        .withFromColumns(new String[] {"customer_id"})
        .withToColumns(new String[] {"id"});
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
}
