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

package org.apache.gravitino.maintenance.optimizer.recommender.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestQLExpressionEvaluator {

  private final QLExpressionEvaluator evaluator = new QLExpressionEvaluator();

  @Test
  void testEvaluateLongWithValidExpression() {
    Map<String, Object> context = new HashMap<>();
    context.put("a", 10);
    context.put("b", 20);

    long result = evaluator.evaluateLong("a + b", context);
    assertEquals(30L, result);
  }

  @Test
  void testEvaluateLongWithDecimalResult() {
    Map<String, Object> context = new HashMap<>();
    context.put("a", 10);
    context.put("b", 3);

    long result = evaluator.evaluateLong("a / b", context);
    assertEquals(3L, result); // Should truncate decimal part
  }

  @Test
  void testEvaluateBoolWithTrueCondition() {
    Map<String, Object> context = new HashMap<>();
    context.put("x", 5);
    context.put("y", 10);

    boolean result = evaluator.evaluateBool("x < y", context);
    assertTrue(result);
  }

  @Test
  void testEvaluateBoolWithFalseCondition() {
    Map<String, Object> context = new HashMap<>();
    context.put("x", 15);
    context.put("y", 10);

    boolean result = evaluator.evaluateBool("x < y", context);
    Assertions.assertFalse(result);
  }

  @Test
  void testEvaluateWithMissingVariable() {
    Map<String, Object> context = new HashMap<>();
    context.put("a", 10);

    assertThrows(
        RuntimeException.class,
        () -> {
          evaluator.evaluateLong("a + b", context);
        });
  }

  @Test
  void testEvaluateWithInvalidExpression() {
    Map<String, Object> context = new HashMap<>();

    assertThrows(
        RuntimeException.class,
        () -> {
          evaluator.evaluateLong("invalid expression", context);
        });
  }

  @Test
  void testEvaluateWithConstantExpression() {
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> evaluator.evaluateLong("1 + 1", null));
  }

  @Test
  void testEvaluateWithNullExpression() {
    Map<String, Object> context = new HashMap<>();

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          evaluator.evaluateLong(null, context);
        });
  }

  @Test
  void testEvaluateWithDifferentVariableTypes() {
    Map<String, Object> context = new HashMap<>();
    context.put("intVal", 10);
    context.put("doubleVal", 5.5);
    context.put("stringVal", "hello");
    context.put("boolVal", true);

    // Test numeric operations
    long numericResult = evaluator.evaluateLong("intVal + doubleVal", context);
    assertEquals(16L, numericResult); // 10 + 5.5 = 15.5 -> rounded to 16

    // Test boolean operations
    boolean boolResult = evaluator.evaluateBool("boolVal && (intVal > 5)", context);
    assertTrue(boolResult);
  }

  @Test
  void testHyphenatedIdentifiersDoNotBreakSubtraction() {
    Map<String, Object> context = new HashMap<>();
    context.put("metric-1", 10);
    context.put("metric2", 4);

    long result = evaluator.evaluateLong("metric-1 - metric2", context);
    assertEquals(6L, result);
  }

  @Test
  void testNegativeLiteralPreserved() {
    Map<String, Object> context = new HashMap<>();
    context.put("x", 2);

    long result = evaluator.evaluateLong("x + -1", context);
    assertEquals(1L, result);
  }

  @Test
  void testHyphenatedIdentifierNotMatchedInsideLargerToken() {
    Map<String, Object> context = new HashMap<>();
    context.put("metric-1", 5);
    context.put("metric-1-extra", 7);

    long result = evaluator.evaluateLong("metric-1 + metric-1-extra", context);
    assertEquals(12L, result);
  }

  @Test
  void testHyphenatedIdentifierNextToDotIsNotRewritten() {
    Map<String, Object> context = new HashMap<>();
    context.put("metric-1", 3);
    context.put("a", 2);

    long result = evaluator.evaluateLong("a + metric-1", context);
    assertEquals(5L, result);
  }

  @Test
  void testTryToEvaluateBoolWithResult() {
    Map<String, Object> context = new HashMap<>();
    context.put("x", 5);
    context.put("y", 10);

    Optional<Boolean> result = evaluator.tryToEvaluateBool("x < y", context);
    assertEquals(Optional.of(true), result);
  }

  @Test
  void testTryToEvaluateBoolWithMissingVariableReturnsEmpty() {
    Map<String, Object> context = new HashMap<>();
    context.put("x", 5);

    Optional<Boolean> result = evaluator.tryToEvaluateBool("x < y", context);
    assertEquals(Optional.empty(), result);
  }

  @Test
  void testTryToEvaluateBoolWithBlankExpression() {
    assertThrows(IllegalArgumentException.class, () -> evaluator.tryToEvaluateBool("", Map.of()));
    assertThrows(IllegalArgumentException.class, () -> evaluator.tryToEvaluateBool(null, Map.of()));
  }

  @Test
  void testRepeatedEvaluationsWithSameHyphenatedKeysProduceCorrectResults() {
    Map<String, Object> context1 = new HashMap<>();
    context1.put("metric-a", 10);
    context1.put("threshold", 5);
    assertTrue(evaluator.evaluateBool("metric-a > threshold", context1));

    // Same hyphenated key set, different values — should reuse cached replacement rule
    Map<String, Object> context2 = new HashMap<>();
    context2.put("metric-a", 3);
    context2.put("threshold", 5);
    Assertions.assertFalse(evaluator.evaluateBool("metric-a > threshold", context2));
  }

  @Test
  void testDifferentHyphenatedKeySetsProduceCorrectResults() {
    Map<String, Object> contextA = new HashMap<>();
    contextA.put("metric-a", 10);
    contextA.put("limit", 5);
    assertEquals(5L, evaluator.evaluateLong("metric-a - limit", contextA));

    // Different hyphenated key set — must not reuse the previous cached rule
    Map<String, Object> contextB = new HashMap<>();
    contextB.put("metric-b", 20);
    contextB.put("limit", 8);
    assertEquals(12L, evaluator.evaluateLong("metric-b - limit", contextB));
  }

  @Test
  void testContextWithoutHyphenatedKeysBypassesReplacementCache() {
    Map<String, Object> context = new HashMap<>();
    context.put("alpha", 7);
    context.put("beta", 3);

    // No hyphenated keys — cache should not be involved, expression evaluated as-is
    assertEquals(10L, evaluator.evaluateLong("alpha + beta", context));
    assertTrue(evaluator.evaluateBool("alpha > beta", context));
  }
}
