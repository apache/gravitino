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
package org.apache.gravitino.policy.rego;

import java.math.BigDecimal;
import java.util.List;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.ColumnMask;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.MaskAction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for {@link RestrictedRegoSourceParser}. */
public class TestRestrictedRegoSourceParser {

  @Test
  void testParsesContextReferences() {
    Comparison comparison =
        (Comparison)
            RestrictedRegoTestUtils.parseFilterExpression("col(\"owner\") == session_user()");

    Assertions.assertEquals(ComparisonOperator.EQ, comparison.operator());
    Assertions.assertEquals("owner", ((Column) comparison.left()).name());
    Assertions.assertInstanceOf(SessionUser.class, comparison.right());

    GroupMembership membership =
        (GroupMembership)
            RestrictedRegoTestUtils.parseFilterExpression("is_group_member(\"auditors\")");
    Assertions.assertEquals("auditors", membership.group());
  }

  @Test
  void testParsesPrecedenceAndLiteralArray() {
    LogicalExpression or =
        (LogicalExpression)
            RestrictedRegoTestUtils.parseFilterExpression(
                "col(\"region\") in [\"US\", \"CA\"] or "
                    + "col(\"level\") >= 3 and not col(\"deleted\") == true");

    Assertions.assertEquals(LogicalOperator.OR, or.operator());
    Assertions.assertEquals(2, or.operands().size());

    Comparison membership = (Comparison) or.operands().get(0);
    Assertions.assertEquals(ComparisonOperator.IN, membership.operator());
    LiteralArray values = (LiteralArray) membership.right();
    Assertions.assertEquals(LiteralType.STRING, values.elementType());
    Assertions.assertEquals(
        List.of("US", "CA"),
        List.of(values.values().get(0).value(), values.values().get(1).value()));

    LogicalExpression and = (LogicalExpression) or.operands().get(1);
    Assertions.assertEquals(LogicalOperator.AND, and.operator());
    Assertions.assertEquals(
        ComparisonOperator.GTE, ((Comparison) and.operands().get(0)).operator());
    Not not = (Not) and.operands().get(1);
    Assertions.assertEquals(ComparisonOperator.EQ, ((Comparison) not.operand()).operator());
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
    Comparison comparison =
        (Comparison) RestrictedRegoTestUtils.parseFilterExpression("col(\"ratio\") == -0.00");
    Literal literal = (Literal) comparison.right();

    Assertions.assertEquals(new BigDecimal("0.00"), literal.value());
    Assertions.assertTrue(literal.negativeZero());

    Comparison positiveZero =
        (Comparison) RestrictedRegoTestUtils.parseFilterExpression("col(\"ratio\") == 0.00");
    Assertions.assertNotEquals(literal, positiveZero.right());

    Comparison one = (Comparison) RestrictedRegoTestUtils.parseFilterExpression("col(\"x\") == 1");
    Comparison onePointZero =
        (Comparison) RestrictedRegoTestUtils.parseFilterExpression("col(\"x\") == 1.00");
    Assertions.assertEquals(one.right(), onePointZero.right());
  }

  @Test
  void testDecodesJsonStringsWithoutNormalizing() {
    Comparison comparison =
        (Comparison)
            RestrictedRegoTestUtils.parseFilterExpression("col(\"a\\\"b.c\") == \"a'b\\n\"");

    Assertions.assertEquals("a\"b.c", ((Column) comparison.left()).name());
    Assertions.assertEquals("a'b\n", ((Literal) comparison.right()).value());

    String escapedPair = "\\" + "uD83D" + "\\" + "uDE00";
    Comparison unicode =
        (Comparison)
            RestrictedRegoTestUtils.parseFilterExpression(
                "col(\"emoji\") == \"" + escapedPair + "\"");
    Assertions.assertEquals("😀", ((Literal) unicode.right()).value());

    String delEscape = RestrictedRegoTestUtils.unicodeEscape('0', '0', '7', 'f');
    Comparison controls =
        (Comparison)
            RestrictedRegoTestUtils.parseFilterExpression(
                "col(\"line\\nname\") == \"a" + delEscape + "b\"");
    Assertions.assertEquals("line\nname", ((Column) controls.left()).name());
    Assertions.assertEquals("a" + (char) 0x7F + "b", ((Literal) controls.right()).value());

    GroupMembership controlGroup =
        (GroupMembership)
            RestrictedRegoTestUtils.parseFilterExpression("is_group_member(\"a\\tb\")");
    Assertions.assertEquals("a\tb", controlGroup.group());
  }

  @Test
  void testRejectsExcessiveNotDepthDuringParsing() {
    String source = "filter := " + "not ".repeat(4090) + "true";

    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new RestrictedRegoSourceParser(source).parseProgram());

    Assertions.assertTrue(
        error
            .getMessage()
            .contains("source depth must not exceed " + RestrictedRegoLimits.MAX_SOURCE_DEPTH));
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
    Assertions.assertDoesNotThrow(() -> RestrictedRegoTestUtils.parseFilterExpression(expression));
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
        IllegalArgumentException.class,
        () -> RestrictedRegoTestUtils.parseFilterExpression(expression));
  }

  @Test
  void testReportsSpecificOneBasedSyntaxLocations() {
    IllegalArgumentException characterError =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> RestrictedRegoExpressionParserFacade.parseRowFilter("filter := @"));
    Assertions.assertTrue(characterError.getMessage().contains("line 1, column 11"));

    IllegalArgumentException chainedComparisonError =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RestrictedRegoExpressionParserFacade.parseRowFilter(
                    "filter := col(\"x\") == 1 == true"));
    Assertions.assertTrue(
        chainedComparisonError.getMessage().contains("chained comparisons are not supported"));
  }
}
