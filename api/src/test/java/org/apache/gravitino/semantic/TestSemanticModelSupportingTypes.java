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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class TestSemanticModelSupportingTypes {

  @Test
  public void testBuildSupportingTypes() {
    DialectExpression dialectExpression =
        DialectExpression.builder()
            .withDialect(Dialects.ANSI_SQL)
            .withExpression("order_amount")
            .build();
    Expression expression =
        Expression.builder().withDialects(new DialectExpression[] {dialectExpression}).build();
    Dimension dimension = Dimension.builder().withIsTime(false).build();
    AIContextObject contextObject =
        AIContextObject.builder()
            .withInstructions("Use certified fields")
            .withSynonyms(new String[] {"amount"})
            .withExamples(new String[] {"Revenue by month"})
            .withAdditionalProperties(Map.of("priority", 1))
            .build();
    CustomExtension extension =
        CustomExtension.builder().withVendorName("example").withData("{\"key\":\"value\"}").build();

    assertArrayEquals(new DialectExpression[] {dialectExpression}, expression.dialects());
    assertEquals(Boolean.FALSE, dimension.isTime());
    assertEquals("Use certified fields", contextObject.instructions());
    assertArrayEquals(new String[] {"amount"}, contextObject.synonyms());
    assertArrayEquals(new String[] {"Revenue by month"}, contextObject.examples());
    assertEquals(BigInteger.ONE, contextObject.additionalProperties().get("priority"));
    assertEquals("example", extension.vendorName());
    assertEquals("{\"key\":\"value\"}", extension.data());
  }

  @Test
  public void testExpressionDefensivelyCopiesDialects() {
    DialectExpression ansi =
        DialectExpression.builder().withDialect(Dialects.ANSI_SQL).withExpression("id").build();
    DialectExpression[] dialects = {ansi};
    Expression expression = Expression.builder().withDialects(dialects).build();

    dialects[0] =
        DialectExpression.builder()
            .withDialect(Dialects.BIGQUERY)
            .withExpression("changed")
            .build();

    assertEquals(Dialects.ANSI_SQL, expression.dialects()[0].dialect());
    assertNotSame(expression.dialects(), expression.dialects());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAIContextVariantsAndAdditionalPropertiesAreImmutable() {
    List<Object> nestedValues = new ArrayList<>();
    nestedValues.add("first");
    Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("values", nestedValues);
    Map<String, Object> additionalProperties = new LinkedHashMap<>();
    additionalProperties.put("nested", nested);

    AIContextObject contextObject =
        AIContextObject.builder().withAdditionalProperties(additionalProperties).build();
    AIContext objectContext = AIContext.of(contextObject);
    AIContext textContext = AIContext.of("Use certified metrics");

    nestedValues.add("changed");
    nested.put("extra", true);
    additionalProperties.put("new", "value");

    assertTrue(textContext.isText());
    assertEquals("Use certified metrics", textContext.text());
    assertNull(textContext.object());
    assertFalse(objectContext.isText());
    assertNull(objectContext.text());
    assertEquals(contextObject, objectContext.object());

    Map<String, Object> storedNested =
        (Map<String, Object>) contextObject.additionalProperties().get("nested");
    assertEquals(List.of("first"), storedNested.get("values"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> contextObject.additionalProperties().put("another", "value"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> ((List<Object>) storedNested.get("values")).add("value"));
  }

  @Test
  public void testOptionalAIContextCollectionsPreserveNullAndEmpty() {
    AIContextObject unset = AIContextObject.builder().build();
    AIContextObject empty =
        AIContextObject.builder().withSynonyms(new String[0]).withExamples(new String[0]).build();

    assertNull(unset.synonyms());
    assertNull(unset.examples());
    assertArrayEquals(new String[0], empty.synonyms());
    assertArrayEquals(new String[0], empty.examples());
    assertNotEquals(unset, empty);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAIContextNumbersAreCanonicalized() {
    Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("short", (short) 2);
    nested.put("float", 0.25F);

    Map<String, Object> additionalProperties = new LinkedHashMap<>();
    additionalProperties.put("byte", (byte) 1);
    additionalProperties.put("integer", 2);
    additionalProperties.put("long", 3L);
    additionalProperties.put("bigInteger", new BigInteger("123456789012345678901234567890"));
    additionalProperties.put("double", 0.125D);
    additionalProperties.put("bigDecimal", new BigDecimal("0.123456789012345678901234567890"));
    additionalProperties.put("nested", nested);
    additionalProperties.put("array", new Number[] {4, 5L, 0.5F});

    AIContextObject contextObject =
        AIContextObject.builder().withAdditionalProperties(additionalProperties).build();
    Map<String, Object> values = contextObject.additionalProperties();

    assertEquals(BigInteger.ONE, values.get("byte"));
    assertEquals(BigInteger.valueOf(2), values.get("integer"));
    assertEquals(BigInteger.valueOf(3), values.get("long"));
    assertEquals(new BigInteger("123456789012345678901234567890"), values.get("bigInteger"));
    assertEquals(new BigDecimal("0.125"), values.get("double"));
    assertEquals(new BigDecimal("0.123456789012345678901234567890"), values.get("bigDecimal"));
    assertEquals(BigInteger.valueOf(2), ((Map<String, Object>) values.get("nested")).get("short"));
    assertEquals(new BigDecimal("0.25"), ((Map<String, Object>) values.get("nested")).get("float"));
    assertEquals(
        List.of(BigInteger.valueOf(4), BigInteger.valueOf(5), new BigDecimal("0.5")),
        values.get("array"));

    AIContextObject equivalentContext =
        AIContextObject.builder()
            .withAdditionalProperties(Map.of("number", Integer.valueOf(1)))
            .build();
    AIContextObject equivalentLongContext =
        AIContextObject.builder()
            .withAdditionalProperties(Map.of("number", Long.valueOf(1)))
            .build();
    assertEquals(equivalentContext, equivalentLongContext);
    assertEquals(equivalentContext.hashCode(), equivalentLongContext.hashCode());
  }

  @Test
  public void testAIContextAdditionalPropertyBounds() {
    Object maximumDepthValue = "value";
    for (int depth = 0; depth < AIContextObject.MAX_ADDITIONAL_PROPERTY_NESTING_DEPTH; depth++) {
      maximumDepthValue = Map.of("nested", maximumDepthValue);
    }
    AIContextObject.builder()
        .withAdditionalProperties(Map.of("maximumDepth", maximumDepthValue))
        .build();

    Object excessiveDepthValue = Map.of("nested", maximumDepthValue);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            AIContextObject.builder()
                .withAdditionalProperties(Map.of("excessiveDepth", excessiveDepthValue))
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            AIContextObject.builder()
                .withAdditionalProperties(Map.of("unsupportedNumber", new AtomicInteger(1)))
                .build());
  }

  @Test
  public void testSupportingTypeValidation() {
    assertThrows(IllegalArgumentException.class, () -> DialectExpression.builder().build());
    assertThrows(IllegalArgumentException.class, () -> Expression.builder().build());
    assertEquals("", AIContext.of("").text());

    DialectExpression ansi =
        DialectExpression.builder().withDialect(Dialects.ANSI_SQL).withExpression("id").build();
    DialectExpression custom =
        DialectExpression.builder().withDialect("TRINO").withExpression("id").build();
    assertEquals("TRINO", custom.dialect());
    assertEquals("ANSI_SQL", Dialects.ANSI_SQL);
    assertThrows(
        IllegalArgumentException.class,
        () -> DialectExpression.builder().withDialect("").withExpression("id").build());
    assertThrows(
        IllegalArgumentException.class,
        () -> Expression.builder().withDialects(new DialectExpression[] {ansi, ansi}).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> Expression.builder().withDialects(new DialectExpression[] {ansi, null}).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> AIContextObject.builder().withSynonyms(new String[] {null}).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> AIContextObject.builder().withExamples(new String[] {null}).build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            AIContextObject.builder()
                .withAdditionalProperties(Map.of("instructions", "duplicate"))
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            AIContextObject.builder()
                .withAdditionalProperties(Map.of("unsupported", new Object()))
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            AIContextObject.builder()
                .withAdditionalProperties(Map.of("not_finite", Double.NaN))
                .build());

    Map<String, Object> cyclic = new LinkedHashMap<>();
    cyclic.put("self", cyclic);
    assertThrows(
        IllegalArgumentException.class,
        () -> AIContextObject.builder().withAdditionalProperties(cyclic).build());
  }
}
