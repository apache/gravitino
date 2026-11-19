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
package org.apache.gravitino.policy.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Structural tests for the operation-specific AST and operator enums. */
class TestSqlExpression {
  @ParameterizedTest
  @MethodSource("sources")
  void testStructuralEqualityAndImmutableOperands(String source) {
    boolean mask = !source.contains("RETURNS BOOLEAN");
    SqlExpression first = RestrictedSqlExpressionParserFacade.parse(source, mask).body();
    SqlExpression second = RestrictedSqlExpressionParserFacade.parse(source, mask).body();
    Assertions.assertEquals(first, second);
    Assertions.assertEquals(first.hashCode(), second.hashCode());
    Assertions.assertEquals(first.toString(), second.toString());
    Assertions.assertNotEquals(first, null);
    Assertions.assertNotEquals(first, new Object());
    Assertions.assertThrows(UnsupportedOperationException.class, () -> first.children().clear());
  }

  private static Stream<String> sources() {
    return Stream.of(
        "() RETURNS BOOLEAN -> TRUE",
        "(x BOOLEAN) RETURNS BOOLEAN -> x",
        "(x INTEGER) RETURNS BOOLEAN -> x = 1",
        "(x BOOLEAN) RETURNS BOOLEAN -> x AND TRUE",
        "(x BOOLEAN) RETURNS BOOLEAN -> NOT x",
        "(x STRING) RETURNS BOOLEAN -> x IS NULL",
        "(x INTEGER) RETURNS BOOLEAN -> x IN (1, NULL)",
        "(x INTEGER) RETURNS BOOLEAN -> x BETWEEN 1 AND 10",
        "(x STRING) RETURNS BOOLEAN -> x LIKE 'A%'",
        "(x STRING) RETURNS BOOLEAN -> CASE x WHEN 'US' THEN TRUE ELSE FALSE END",
        "(x STRING) RETURNS BOOLEAN -> CASE WHEN x IS NULL THEN FALSE END",
        "(x STRING) RETURNS STRING -> show_last_4(x)");
  }

  @Test
  void testComparisonOperatorsAndAliases() {
    for (ComparisonOperator operator : ComparisonOperator.values()) {
      Assertions.assertSame(operator, ComparisonOperator.fromToken(operator.sourceToken()));
      Assertions.assertEquals(
          operator != ComparisonOperator.EQ && operator != ComparisonOperator.NEQ,
          operator.ordered());
    }
    Assertions.assertSame(ComparisonOperator.NEQ, ComparisonOperator.fromToken("!="));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ComparisonOperator.fromToken("=="));
    Assertions.assertEquals(SqlFunction.SHOW_LAST_4, SqlFunction.fromName("Show_Last_4"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> SqlFunction.fromName("unknown"));
  }

  @Test
  void testOperationIdentityAndAccessorMethods() {
    Literal one = new Literal(LiteralType.NUMBER, "1", null);
    Literal two = new Literal(LiteralType.NUMBER, "2", null);
    Literal three = new Literal(LiteralType.NUMBER, "3", null);
    Comparison comparison = new Comparison(ComparisonOperator.LT, one, two);
    Assertions.assertSame(one, comparison.left());
    Assertions.assertSame(two, comparison.right());
    Assertions.assertNotEquals(comparison, new Comparison(ComparisonOperator.LTE, one, two));
    Assertions.assertSame(one, new Not(one).operand());
    Assertions.assertSame(one, new IsNull(one, true).operand());
    Assertions.assertTrue(new IsNull(one, true).negated());
    Assertions.assertNotEquals(new IsNull(one, true), new IsNull(one, false));
    Between between = new Between(one, two, three);
    Assertions.assertSame(one, between.operand());
    Assertions.assertSame(two, between.lower());
    Assertions.assertSame(three, between.upper());
    In in = new In(one, Arrays.asList(two, three));
    Assertions.assertSame(one, in.operand());
    Assertions.assertEquals(Arrays.asList(two, three), in.values());
    Assertions.assertNotEquals(between, in);
    Literal pattern = new Literal(LiteralType.STRING, "A%", SqlType.STRING);
    Like like = new Like(one, pattern);
    Assertions.assertSame(one, like.operand());
    Assertions.assertSame(pattern, like.pattern());
    Assertions.assertNotEquals(one, new Literal(LiteralType.NUMBER, "2", null));
    Assertions.assertNotEquals(one, new InputReference("X", SqlType.INTEGER));
  }

  @Test
  void testCopiesMutableConstructionInputs() {
    Literal yes = new Literal(LiteralType.BOOLEAN, "TRUE", SqlType.BOOLEAN);
    List<SqlExpression> operands = new ArrayList<>(Arrays.asList(yes, yes));
    LogicalExpression logical = new LogicalExpression(LogicalOperator.AND, operands);
    operands.clear();
    Assertions.assertEquals(2, logical.operands().size());
    FunctionCall call = new FunctionCall(SqlFunction.CURRENT_USER, operands);
    operands.add(yes);
    Assertions.assertTrue(call.arguments().isEmpty());
    CaseBranch branch = new CaseBranch(yes, yes);
    List<CaseBranch> branches = new ArrayList<>(Collections.singletonList(branch));
    CaseExpression expression = new CaseExpression(null, branches, null);
    branches.clear();
    Assertions.assertEquals(1, expression.branches().size());
    Assertions.assertSame(yes, expression.branches().get(0).condition());
    Assertions.assertSame(yes, expression.results().get(0));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> expression.branches().clear());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> expression.results().clear());
    Assertions.assertNull(expression.selector());
    Assertions.assertNull(expression.fallback());
    Assertions.assertEquals(1, expression.depth());
    Assertions.assertEquals(0, yes.depth());
    Assertions.assertEquals(1, call.depth());
  }
}
