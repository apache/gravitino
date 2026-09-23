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
package org.apache.gravitino.policy.expression;

import java.math.BigDecimal;
import java.util.List;
import org.apache.gravitino.policy.expression.CanonicalExpression.Column;
import org.apache.gravitino.policy.expression.CanonicalExpression.GroupMembership;
import org.apache.gravitino.policy.expression.CanonicalExpression.Literal;
import org.apache.gravitino.policy.expression.CanonicalExpression.LiteralArray;
import org.apache.gravitino.policy.expression.CanonicalExpression.LiteralType;
import org.apache.gravitino.policy.expression.CanonicalExpression.Operation;
import org.apache.gravitino.policy.expression.CanonicalExpression.Operator;
import org.apache.gravitino.policy.expression.CanonicalExpression.SessionUser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for {@link RestrictedRegoExpressionParserFacade}. */
public class TestRestrictedRegoExpressionParserFacade {

  @Test
  void testParsesContextReferences() {
    Operation comparison =
        (Operation) RestrictedRegoExpressionParserFacade.parse("col(\"owner\") == session_user()");

    Assertions.assertEquals(Operator.EQ, comparison.op());
    Assertions.assertEquals("owner", ((Column) comparison.left()).name());
    Assertions.assertInstanceOf(SessionUser.class, comparison.right());

    GroupMembership membership =
        (GroupMembership)
            RestrictedRegoExpressionParserFacade.parse("is_group_member(\"auditors\")");
    Assertions.assertEquals("auditors", membership.group());
  }

  @Test
  void testParsesPrecedenceAndLiteralArray() {
    Operation or =
        (Operation)
            RestrictedRegoExpressionParserFacade.parse(
                "col(\"region\") in [\"US\", \"CA\"] or "
                    + "col(\"level\") >= 3 and not col(\"deleted\") == true");

    Assertions.assertEquals(Operator.OR, or.op());
    Assertions.assertEquals(2, or.operands().size());

    Operation membership = (Operation) or.operands().get(0);
    Assertions.assertEquals(Operator.IN, membership.op());
    LiteralArray values = (LiteralArray) membership.right();
    Assertions.assertEquals(LiteralType.STRING, values.elementType());
    Assertions.assertEquals(
        List.of("US", "CA"),
        List.of(values.values().get(0).value(), values.values().get(1).value()));

    Operation and = (Operation) or.operands().get(1);
    Assertions.assertEquals(Operator.AND, and.op());
    Assertions.assertEquals(Operator.GTE, ((Operation) and.operands().get(0)).op());
    Operation not = (Operation) and.operands().get(1);
    Assertions.assertEquals(Operator.NOT, not.op());
    Assertions.assertEquals(Operator.EQ, ((Operation) not.operand()).op());
  }

  @Test
  void testRetainsExactNumbersAndNegativeZero() {
    Operation comparison =
        (Operation) RestrictedRegoExpressionParserFacade.parse("col(\"ratio\") == -0.00");
    Literal literal = (Literal) comparison.right();

    Assertions.assertEquals(new BigDecimal("0.00"), literal.value());
    Assertions.assertTrue(literal.negativeZero());

    Operation positiveZero =
        (Operation) RestrictedRegoExpressionParserFacade.parse("col(\"ratio\") == 0.00");
    Assertions.assertNotEquals(literal, positiveZero.right());

    Operation one = (Operation) RestrictedRegoExpressionParserFacade.parse("col(\"x\") == 1");
    Operation onePointZero =
        (Operation) RestrictedRegoExpressionParserFacade.parse("col(\"x\") == 1.00");
    Assertions.assertEquals(one.right(), onePointZero.right());
  }

  @Test
  void testDecodesJsonStringsWithoutNormalizing() {
    Operation comparison =
        (Operation) RestrictedRegoExpressionParserFacade.parse("col(\"a\\\"b.c\") == \"a'b\\n\"");

    Assertions.assertEquals("a\"b.c", ((Column) comparison.left()).name());
    Assertions.assertEquals("a'b\n", ((Literal) comparison.right()).value());

    String escapedPair = "\\" + "uD83D" + "\\" + "uDE00";
    Operation unicode =
        (Operation)
            RestrictedRegoExpressionParserFacade.parse("col(\"emoji\") == \"" + escapedPair + "\"");
    Assertions.assertEquals("😀", ((Literal) unicode.right()).value());
  }

  @Test
  void testContextOnlyConditions() {
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoExpressionParserFacade.parseContextCondition(
                "not is_group_member(\"pii_unmasked\") " + "and session_user() != \"service\""));
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoExpressionParserFacade.parseContextCondition(
                "session_user() in [\"alice\", \"bob\"]"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoExpressionParserFacade.parseContextCondition(
                "col(\"region\") == \"US\""));
  }

  @Test
  void testSourceDepthLimit() {
    CanonicalExpression depthEight =
        RestrictedRegoExpressionParserFacade.parse(
            "not not not not not not not col(\"active\") == true");
    Assertions.assertEquals(8, depthEight.depth());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoExpressionParserFacade.parse(
                "not not not not not not not not col(\"active\") == true"));
  }

  @Test
  void testChecksSourceDepthBeforeBooleanFlattening() {
    CanonicalExpression depthEight =
        RestrictedRegoExpressionParserFacade.parse(
            "true and true and true and true and true and true and true and true");
    Assertions.assertEquals(8, depthEight.depth());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoExpressionParserFacade.parse(
                "true and true and true and true and true and true and true and true and true"));
  }

  @Test
  void testRejectsInvalidUnicodeScalarsAndColumnNul() {
    String isolatedSurrogate = "\\" + "uD800";
    String nul = "\\" + "u0000";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoExpressionParserFacade.parse(
                "col(\"" + isolatedSurrogate + "\") == \"x\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoExpressionParserFacade.parse(
                "col(\"x\") == \"" + isolatedSurrogate + "\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedRegoExpressionParserFacade.parse("col(\"" + nul + "\") == \"x\""));
  }

  @Test
  void testResourceLimits() {
    String maximumSource = "true" + " ".repeat(16 * 1024 - 4);
    Assertions.assertDoesNotThrow(() -> RestrictedRegoExpressionParserFacade.parse(maximumSource));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedRegoExpressionParserFacade.parse(maximumSource + " "));

    String maximumString = "a".repeat(4 * 1024);
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoExpressionParserFacade.parse(
                "col(\"value\") == \"" + maximumString + "\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoExpressionParserFacade.parse(
                "col(\"value\") == \"" + maximumString + "a\""));

    String maximumUnicodeString = "😀".repeat(1024);
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoExpressionParserFacade.parse(
                "col(\"value\") == \"" + maximumUnicodeString + "\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoExpressionParserFacade.parse(
                "col(\"value\") == \"" + maximumUnicodeString + "😀\""));

    Assertions.assertDoesNotThrow(
        () -> RestrictedRegoExpressionParserFacade.parse("col(\"value\") in " + literalArray(256)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedRegoExpressionParserFacade.parse("col(\"value\") in " + literalArray(257)));

    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoExpressionParserFacade.parse(
                "not (" + balancedAndExpression(0, 64) + ")"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedRegoExpressionParserFacade.parse(balancedAndExpression(0, 65)));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "col(\"owner\") == session_user()",
        "session_user() != col(\"owner\")",
        "session_user() == \"alice\"",
        "\"alice\" != session_user()",
        "session_user() in [\"alice\", \"bob\"]",
        "is_group_member(\"auditors\")",
        "col(\"region\") in [\"US\", \"CA\"]",
        "not col(\"deleted\") == true",
        "col(\"level\") >= 3 and col(\"region\") == \"US\"",
        "col(\"nullable\") == null",
        "null != col(\"nullable\")",
        "true",
        "false"
      })
  void testAcceptsAllowlistedExpressions(String expression) {
    Assertions.assertDoesNotThrow(() -> RestrictedRegoExpressionParserFacade.parse(expression));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        " ",
        "col(\"owner\")",
        "session_user()",
        "\"alice\"",
        "1",
        "null",
        "[\"US\"]",
        "is_group_member(\"\")",
        "is_group_member(\"auditors\") == true",
        "col(\"left\") == col(\"right\")",
        "1 == 1",
        "col(\"active\") > true",
        "false <= col(\"active\")",
        "session_user() < \"bob\"",
        "session_user() == true",
        "session_user() == session_user()",
        "col(\"region\") in []",
        "col(\"region\") in [\"US\", 1]",
        "col(\"region\") in [\"US\", null]",
        "col(\"region\") == [\"US\"]",
        "col(\"level\") + 1 > 3",
        "col(\"level\") == 1e3",
        "col(\"level\") == +1",
        "col(\"level\") == 01",
        "col(\"a\") < 2 < 3",
        "col(\"owner\") == \"alice\" trailing",
        "identity(\"user\") == col(\"owner\")",
        "input.user == \"alice\"",
        "col(\"x\") == \"unterminated",
        "col(\"x\") == \"line\nbreak\""
      })
  void testRejectsUnsupportedOrMalformedExpressions(String expression) {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedRegoExpressionParserFacade.parse(expression));
  }

  private static String literalArray(int elementCount) {
    StringBuilder result = new StringBuilder("[");
    for (int index = 0; index < elementCount; index++) {
      if (index > 0) {
        result.append(',');
      }
      result.append(index);
    }
    return result.append(']').toString();
  }

  private static String balancedAndExpression(int start, int end) {
    if (end - start == 1) {
      return "col(\"column_" + start + "\") == " + start;
    }

    int middle = start + (end - start) / 2;
    return "("
        + balancedAndExpression(start, middle)
        + " and "
        + balancedAndExpression(middle, end)
        + ")";
  }
}
