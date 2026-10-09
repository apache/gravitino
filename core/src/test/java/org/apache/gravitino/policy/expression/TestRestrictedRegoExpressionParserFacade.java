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
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.gravitino.policy.expression.CanonicalExpression.Column;
import org.apache.gravitino.policy.expression.CanonicalExpression.Comparison;
import org.apache.gravitino.policy.expression.CanonicalExpression.GroupMembership;
import org.apache.gravitino.policy.expression.CanonicalExpression.Literal;
import org.apache.gravitino.policy.expression.CanonicalExpression.LiteralArray;
import org.apache.gravitino.policy.expression.CanonicalExpression.LiteralType;
import org.apache.gravitino.policy.expression.CanonicalExpression.Logical;
import org.apache.gravitino.policy.expression.CanonicalExpression.Not;
import org.apache.gravitino.policy.expression.CanonicalExpression.Operator;
import org.apache.gravitino.policy.expression.CanonicalExpression.SessionUser;
import org.apache.gravitino.policy.expression.RestrictedRegoProgram.ColumnMask;
import org.apache.gravitino.policy.expression.RestrictedRegoProgram.MaskAction;
import org.apache.gravitino.policy.expression.RestrictedRegoProgram.RowFilter;
import org.apache.gravitino.policy.expression.RestrictedRegoProgram.RuleType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for {@link RestrictedRegoExpressionParserFacade}. */
public class TestRestrictedRegoExpressionParserFacade {

  @Test
  void testParsesTypedRules() {
    RowFilter filter =
        RestrictedRegoExpressionParserFacade.parseRowFilter(
            "filter := col(\"owner\") == session_user()");
    Assertions.assertEquals(RuleType.FILTER, filter.ruleType());
    Assertions.assertFalse(filter.isConditional());
    Assertions.assertInstanceOf(Comparison.class, filter.fallback());

    ColumnMask mask =
        RestrictedRegoExpressionParserFacade.parseColumnMask("mask := action(\"show-last-4\")");
    Assertions.assertEquals(RuleType.MASK, mask.ruleType());
    Assertions.assertFalse(mask.isConditional());
    Assertions.assertEquals(MaskAction.SHOW_LAST_4, mask.fallback());

    Assertions.assertInstanceOf(
        RowFilter.class, RestrictedRegoExpressionParserFacade.parse("filter := true"));
    Assertions.assertInstanceOf(
        ColumnMask.class,
        RestrictedRegoExpressionParserFacade.parse("mask := action(\"mask-alphanum\")"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoExpressionParserFacade.parseRowFilter("mask := action(\"show-last-4\")"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedRegoExpressionParserFacade.parseColumnMask("filter := true"));
  }

  @Test
  void testParsesContextReferences() {
    Comparison comparison = (Comparison) parseFilterExpression("col(\"owner\") == session_user()");

    Assertions.assertEquals(Operator.EQ, comparison.operator());
    Assertions.assertEquals("owner", ((Column) comparison.left()).name());
    Assertions.assertInstanceOf(SessionUser.class, comparison.right());

    GroupMembership membership =
        (GroupMembership) parseFilterExpression("is_group_member(\"auditors\")");
    Assertions.assertEquals("auditors", membership.group());
  }

  @Test
  void testParsesPrecedenceAndLiteralArray() {
    Logical or =
        (Logical)
            parseFilterExpression(
                "col(\"region\") in [\"US\", \"CA\"] or "
                    + "col(\"level\") >= 3 and not col(\"deleted\") == true");

    Assertions.assertEquals(Operator.OR, or.operator());
    Assertions.assertEquals(2, or.operands().size());

    Comparison membership = (Comparison) or.operands().get(0);
    Assertions.assertEquals(Operator.IN, membership.operator());
    LiteralArray values = (LiteralArray) membership.right();
    Assertions.assertEquals(LiteralType.STRING, values.elementType());
    Assertions.assertEquals(
        List.of("US", "CA"),
        List.of(values.values().get(0).value(), values.values().get(1).value()));

    Logical and = (Logical) or.operands().get(1);
    Assertions.assertEquals(Operator.AND, and.operator());
    Assertions.assertEquals(Operator.GTE, ((Comparison) and.operands().get(0)).operator());
    Not not = (Not) and.operands().get(1);
    Assertions.assertEquals(Operator.EQ, ((Comparison) not.operand()).operator());
  }

  @Test
  void testParsesConditionalRowFilterAndLowersFirstMatch() {
    RowFilter filter =
        RestrictedRegoExpressionParserFacade.parseRowFilter(
            "filter := col(\"tenant\") == session_user() if is_group_member(\"member\") "
                + "else := col(\"public\") == true if session_user() == \"guest\" "
                + "else := false");

    Assertions.assertTrue(filter.isConditional());
    Assertions.assertEquals(2, filter.branches().size());
    Assertions.assertInstanceOf(GroupMembership.class, filter.branches().get(0).condition());
    Assertions.assertEquals(
        Operator.EQ, ((Comparison) filter.branches().get(1).condition()).operator());
    Assertions.assertEquals(false, ((Literal) filter.fallback()).value());
    Assertions.assertThrows(UnsupportedOperationException.class, () -> filter.branches().add(null));

    Logical lowered = (Logical) filter.lower();
    Assertions.assertEquals(Operator.OR, lowered.operator());
    Assertions.assertEquals(3, lowered.operands().size());
    Assertions.assertEquals(Operator.AND, ((Logical) lowered.operands().get(0)).operator());
    Logical secondBranch = (Logical) lowered.operands().get(1);
    Assertions.assertEquals(Operator.AND, secondBranch.operator());
    Assertions.assertInstanceOf(Not.class, secondBranch.operands().get(0));
    Logical fallback = (Logical) lowered.operands().get(2);
    Assertions.assertEquals(Operator.AND, fallback.operator());
    Assertions.assertInstanceOf(Not.class, fallback.operands().get(0));
    Assertions.assertInstanceOf(Not.class, fallback.operands().get(1));
    Assertions.assertEquals(4, lowered.depth());
  }

  @Test
  void testParsesConditionalColumnMask() {
    ColumnMask mask =
        RestrictedRegoExpressionParserFacade.parseColumnMask(
            "mask := action(\"show-last-4\") if is_group_member(\"support\") "
                + "else := action(\"sha-256-query-local\") if session_user() == \"service\" "
                + "else := action(\"replace-with-null\")");

    Assertions.assertTrue(mask.isConditional());
    Assertions.assertEquals(2, mask.branches().size());
    Assertions.assertEquals(MaskAction.SHOW_LAST_4, mask.branches().get(0).action());
    Assertions.assertEquals(MaskAction.SHA_256_QUERY_LOCAL, mask.branches().get(1).action());
    Assertions.assertEquals(MaskAction.REPLACE_WITH_NULL, mask.fallback());
    Assertions.assertInstanceOf(GroupMembership.class, mask.branches().get(0).condition());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "mask-alphanum",
        "mask-to-fixed-value",
        "replace-with-null",
        "show-first-4",
        "show-last-4",
        "truncate-to-year",
        "truncate-to-month",
        "sha-256-global",
        "sha-256-query-local"
      })
  void testAcceptsAllMaskActions(String action) {
    ColumnMask mask =
        RestrictedRegoExpressionParserFacade.parseColumnMask("mask := action(\"" + action + "\")");
    Assertions.assertEquals(action, mask.fallback().value());
  }

  @Test
  void testRetainsExactNumbersAndNegativeZero() {
    Comparison comparison = (Comparison) parseFilterExpression("col(\"ratio\") == -0.00");
    Literal literal = (Literal) comparison.right();

    Assertions.assertEquals(new BigDecimal("0.00"), literal.value());
    Assertions.assertTrue(literal.negativeZero());

    Comparison positiveZero = (Comparison) parseFilterExpression("col(\"ratio\") == 0.00");
    Assertions.assertNotEquals(literal, positiveZero.right());

    Comparison one = (Comparison) parseFilterExpression("col(\"x\") == 1");
    Comparison onePointZero = (Comparison) parseFilterExpression("col(\"x\") == 1.00");
    Assertions.assertEquals(one.right(), onePointZero.right());
  }

  @Test
  void testDecodesJsonStringsWithoutNormalizing() {
    Comparison comparison = (Comparison) parseFilterExpression("col(\"a\\\"b.c\") == \"a'b\\n\"");

    Assertions.assertEquals("a\"b.c", ((Column) comparison.left()).name());
    Assertions.assertEquals("a'b\n", ((Literal) comparison.right()).value());

    String escapedPair = "\\" + "uD83D" + "\\" + "uDE00";
    Comparison unicode =
        (Comparison) parseFilterExpression("col(\"emoji\") == \"" + escapedPair + "\"");
    Assertions.assertEquals("😀", ((Literal) unicode.right()).value());
  }

  @Test
  void testSourceDepthLimit() {
    CanonicalExpression depthEight =
        parseFilterExpression("not not not not not not not col(\"active\") == true");
    Assertions.assertEquals(8, depthEight.depth());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("not not not not not not not not col(\"active\") == true"));
  }

  @Test
  void testFlattensAssociativeBooleanChains() {
    Logical and =
        (Logical)
            parseFilterExpression(
                "true and true and true and true and true and true and true and true and true");
    Assertions.assertEquals(Operator.AND, and.operator());
    Assertions.assertEquals(9, and.operands().size());
    Assertions.assertEquals(2, and.depth());
  }

  @Test
  void testChecksLoweredRowFilterDepthAtSaveTime() {
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoExpressionParserFacade.parseRowFilter(
                "filter := true if not not not not col(\"active\") == true else := false"));

    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RestrictedRegoExpressionParserFacade.parseRowFilter(
                    "filter := true if not not not not not col(\"active\") == true "
                        + "else := false"));
    Assertions.assertTrue(error.getMessage().contains("lowered row-filter depth"));
  }

  @Test
  void testRejectsInvalidUnicodeScalarsAndColumnNul() {
    String isolatedSurrogate = "\\" + "uD800";
    String nul = "\\" + "u0000";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("col(\"" + isolatedSurrogate + "\") == \"x\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("col(\"x\") == \"" + isolatedSurrogate + "\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("col(\"" + nul + "\") == \"x\""));
  }

  @Test
  void testResourceLimits() {
    String sourcePrefix = "filter := true";
    String maximumSource =
        sourcePrefix + " ".repeat(16 * 1024 - sourcePrefix.getBytes(StandardCharsets.UTF_8).length);
    Assertions.assertDoesNotThrow(
        () -> RestrictedRegoExpressionParserFacade.parseRowFilter(maximumSource));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedRegoExpressionParserFacade.parseRowFilter(maximumSource + " "));

    String maximumString = "a".repeat(4 * 1024);
    Assertions.assertDoesNotThrow(
        () -> parseFilterExpression("col(\"value\") == \"" + maximumString + "\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("col(\"value\") == \"" + maximumString + "a\""));

    String maximumUnicodeString = "😀".repeat(1024);
    Assertions.assertDoesNotThrow(
        () -> parseFilterExpression("col(\"value\") == \"" + maximumUnicodeString + "\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("col(\"value\") == \"" + maximumUnicodeString + "😀\""));

    Assertions.assertDoesNotThrow(
        () -> parseFilterExpression("col(\"value\") in " + literalArray(256)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("col(\"value\") in " + literalArray(257)));

    Assertions.assertDoesNotThrow(
        () -> parseFilterExpression("not (" + balancedAndExpression(0, 64) + ")"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> parseFilterExpression(balancedAndExpression(0, 65)));

    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoExpressionParserFacade.parseRowFilter(
                "filter := "
                    + balancedAndExpression(0, 16)
                    + " if "
                    + balancedAndExpression(16, 32)
                    + " else := true"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoExpressionParserFacade.parseRowFilter(
                "filter := "
                    + balancedAndExpression(0, 17)
                    + " if "
                    + balancedAndExpression(17, 34)
                    + " else := true"));
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
    Assertions.assertDoesNotThrow(() -> parseFilterExpression(expression));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
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
        "notcol(\"active\") == true",
        "trueand false",
        "col(\"level\") == 1and col(\"region\") == \"US\"",
        "session_userx() == \"alice\"",
        "col(\"owner\") == \"alice\" trailing",
        "identity(\"user\") == col(\"owner\")",
        "input.user == \"alice\"",
        "col(\"x\") == \"unterminated",
        "col(\"x\") == \"line\nbreak\""
      })
  void testRejectsUnsupportedOrMalformedExpressions(String expression) {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> parseFilterExpression(expression));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        " ",
        "true",
        "result := true",
        "package restrictions",
        "import data.rules",
        "filter := true filter := false",
        "filter := true { true }",
        "filter := true # comment",
        "filter := true if is_group_member(\"a\")",
        "filter := true if true else := false if false",
        "filter := true then false else := false",
        "filter := \"not-boolean\"",
        "mask := \"show-last-4\"",
        "mask := action(\"unknown\")",
        "mask := action(\"show-last-4\") if true",
        "mask := action(\"show-last-4\") if col(\"region\") == \"US\" else := action(\"mask-alphanum\")",
        "mask := action(\"show-last-4\") if true else := action(\"mask-alphanum\") if col(\"x\") == 1 else := action(\"replace-with-null\")",
        "mask := true",
        "filter := action(\"show-last-4\")"
      })
  void testRejectsUnsupportedOrMalformedPrograms(String source) {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> RestrictedRegoExpressionParserFacade.parse(source));
  }

  @Test
  void testReportsSpecificOneBasedSyntaxLocations() {
    IllegalArgumentException identifierError =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RestrictedRegoExpressionParserFacade.parseRowFilter("filter := region == \"US\""));
    Assertions.assertTrue(identifierError.getMessage().contains("line 1, column 11"));
    Assertions.assertTrue(identifierError.getMessage().contains("unsupported identifier region"));

    IllegalArgumentException characterError =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> RestrictedRegoExpressionParserFacade.parseRowFilter("filter := @"));
    Assertions.assertTrue(characterError.getMessage().contains("line 1, column 11"));

    IllegalArgumentException leadingZeroError =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RestrictedRegoExpressionParserFacade.parseRowFilter("filter := col(\"x\") == 01"));
    Assertions.assertTrue(leadingZeroError.getMessage().contains("leading zeros"));

    IllegalArgumentException chainedComparisonError =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RestrictedRegoExpressionParserFacade.parseRowFilter(
                    "filter := col(\"x\") == 1 == true"));
    Assertions.assertTrue(
        chainedComparisonError.getMessage().contains("chained comparisons are not supported"));
  }

  private static CanonicalExpression parseFilterExpression(String expression) {
    return RestrictedRegoExpressionParserFacade.parseRowFilter("filter := " + expression)
        .fallback();
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
