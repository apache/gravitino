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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestMetricSupportingTypes {

  @Test
  public void testDialectsMatchPinnedOssieSchema() {
    assertEquals("ANSI_SQL", MetricDialects.ANSI_SQL);
    assertEquals("SNOWFLAKE", MetricDialects.SNOWFLAKE);
    assertEquals("MDX", MetricDialects.MDX);
    assertEquals("TABLEAU", MetricDialects.TABLEAU);
    assertEquals("DATABRICKS", MetricDialects.DATABRICKS);
    assertEquals("MAQL", MetricDialects.MAQL);
    assertEquals("BIGQUERY", MetricDialects.BIGQUERY);
  }

  @Test
  public void testDialectIsExtensibleString() {
    DialectExpression expression =
        DialectExpression.builder()
            .withDialect("CUSTOM_DIALECT")
            .withExpression("orders.amount")
            .build();

    assertEquals("CUSTOM_DIALECT", expression.dialect());
  }

  @Test
  public void testBuildFieldAndExpression() {
    List<DialectExpression> dialects = new ArrayList<>();
    dialects.add(
        DialectExpression.builder()
            .withDialect(MetricDialects.ANSI_SQL)
            .withExpression("orders.amount")
            .build());
    Expression expression = Expression.builder().withDialects(dialects).build();
    Dimension dimension = Dimension.builder().withTime(false).build();
    CustomExtension extension =
        CustomExtension.builder().withVendorName("example").withData("{\"key\":1}").build();
    Field field =
        Field.builder()
            .withName("amount")
            .withExpression(expression)
            .withDimension(dimension)
            .withLabel("Amount")
            .withDescription("Order amount")
            .withCustomExtensions(Collections.singletonList(extension))
            .build();

    dialects.clear();

    assertEquals("amount", field.name());
    assertEquals("orders.amount", field.expression().dialects().get(0).expression());
    assertEquals(Boolean.FALSE, field.dimension().isTime());
    assertEquals("Amount", field.label());
    assertEquals(Collections.singletonList(extension), field.customExtensions());
    assertThrows(UnsupportedOperationException.class, () -> expression.dialects().clear());
  }

  @Test
  public void testAIContextForms() {
    AIContext textContext = AIContext.of("Use fiscal calendar");
    AIContext objectContext =
        AIContext.builder()
            .withInstructions("Use certified fields")
            .withSynonyms(Collections.singletonList("sales"))
            .withExamples(Collections.singletonList("Revenue by month"))
            .withAdditionalProperties(Collections.singletonMap("audience", "finance"))
            .build();

    assertTrue(textContext.isText());
    assertEquals("Use fiscal calendar", textContext.text());
    assertFalse(objectContext.isText());
    assertNull(objectContext.text());
    assertEquals("Use certified fields", objectContext.instructions());
    assertEquals("finance", objectContext.additionalProperties().get("audience"));
  }

  @Test
  public void testRejectInvalidSupportingTypes() {
    assertThrows(
        IllegalArgumentException.class,
        () -> Expression.builder().withDialects(Collections.emptyList()).build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DialectExpression.builder()
                .withDialect(MetricDialects.ANSI_SQL)
                .withExpression(" ")
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () -> DialectExpression.builder().withDialect(" ").withExpression("amount").build());
    assertThrows(IllegalArgumentException.class, () -> Field.builder().withName("amount").build());
    assertThrows(
        IllegalArgumentException.class,
        () -> CustomExtension.builder().withVendorName("example").withData(" ").build());
  }

  @Test
  public void testRejectReservedAIContextAdditionalProperty() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("instructions", "duplicate");

    assertThrows(
        IllegalArgumentException.class,
        () -> AIContext.builder().withAdditionalProperties(properties).build());
  }
}
