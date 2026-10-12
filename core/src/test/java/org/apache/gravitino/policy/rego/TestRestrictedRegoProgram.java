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

import org.apache.gravitino.policy.rego.RestrictedRegoProgram.RowFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link RestrictedRegoProgram}. */
public class TestRestrictedRegoProgram {

  @Test
  void testLowersConditionalRowFilterWithFirstMatchSemantics() {
    RowFilter filter =
        RestrictedRegoExpressionParserFacade.parseRowFilter(
            "filter := col(\"tenant\") == session_user() if is_group_member(\"member\") "
                + "else := col(\"public\") == true if session_user() == \"guest\" "
                + "else := false");

    Assertions.assertTrue(filter.isConditional());
    Assertions.assertEquals(2, filter.branches().size());
    Assertions.assertInstanceOf(GroupMembership.class, filter.branches().get(0).condition());
    Assertions.assertEquals(
        ComparisonOperator.EQ, ((Comparison) filter.branches().get(1).condition()).operator());
    Assertions.assertEquals(false, ((Literal) filter.fallback()).value());
    Assertions.assertThrows(UnsupportedOperationException.class, () -> filter.branches().add(null));

    CanonicalExpression expected =
        RestrictedRegoTestUtils.parseFilterExpression(
            "(is_group_member(\"member\") and col(\"tenant\") == session_user()) or "
                + "(not is_group_member(\"member\") and "
                + "((session_user() == \"guest\" and col(\"public\") == true) or "
                + "(not session_user() == \"guest\" and false)))");
    Assertions.assertEquals(expected, filter.lower());
    Assertions.assertEquals(6, filter.lower().depth());
  }

  @Test
  void testDoesNotApplySourceDepthLimitToGeneratedPredicate() {
    RowFilter filter =
        RestrictedRegoExpressionParserFacade.parseRowFilter(
            "filter := true if not not not not not not not col(\"active\") == true "
                + "else := false");

    Assertions.assertTrue(filter.lower().depth() > RestrictedRegoLimits.MAX_SOURCE_DEPTH);
  }

  @Test
  void testFoldsDoubleNegationIntroducedByLowering() {
    RowFilter filter =
        RestrictedRegoExpressionParserFacade.parseRowFilter(
            "filter := true if not is_group_member(\"g\") else := false");

    CanonicalExpression expected =
        RestrictedRegoTestUtils.parseFilterExpression(
            "(not is_group_member(\"g\") and true) or " + "(is_group_member(\"g\") and false)");
    Assertions.assertEquals(expected, filter.lower());
  }

  @Test
  void testLowersConditionalBranchesWithLinearNodeGrowth() {
    RowFilter filter =
        RestrictedRegoExpressionParserFacade.parseRowFilter(
            RestrictedRegoTestUtils.conditionalFilterWithBranches(24));

    Assertions.assertEquals(265, RestrictedRegoTestUtils.countExpressionNodes(filter.lower()));
  }

  @Test
  void testLowersMaximumNumberOfMinimalBranchesWithinSourceNodeLimit() {
    StringBuilder source = new StringBuilder();
    for (int index = 0; index < 127; index++) {
      source.append(index == 0 ? "filter := true if true" : " else := true if true");
    }
    source.append(" else := false");

    RowFilter filter = RestrictedRegoExpressionParserFacade.parseRowFilter(source.toString());
    CanonicalExpression lowered = filter.lower();

    Assertions.assertEquals(127, filter.branches().size());
    Assertions.assertEquals(256, lowered.depth());
    Assertions.assertEquals(890, RestrictedRegoTestUtils.countExpressionNodes(lowered));
  }

  @Test
  void testDoesNotApplySourceArrayLimitToGeneratedPredicate() {
    RowFilter filter =
        Assertions.assertDoesNotThrow(
            () ->
                RestrictedRegoExpressionParserFacade.parseRowFilter(
                    "filter := true if col(\"value\") in "
                        + RestrictedRegoTestUtils.literalArray(256)
                        + " else := true if is_group_member(\"fallback\") else := false"));

    Assertions.assertEquals(512, RestrictedRegoTestUtils.countArrayElements(filter.lower()));
  }
}
