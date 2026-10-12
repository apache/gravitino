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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.ColumnMask;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.FilterBranch;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.MaskBranch;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.RowFilter;

/** Applies resource limits to a parsed {@code restricted-rego-v1} source program. */
final class RestrictedRegoSourceValidator {

  private RestrictedRegoSourceValidator() {}

  static void validate(RestrictedRegoProgram program) {
    program.validate();
    List<CanonicalExpression> expressions = expressions(program);
    int nodeCount = 0;
    int arrayElementCount = 0;
    for (CanonicalExpression expression : expressions) {
      Preconditions.checkArgument(
          expression.depth() <= RestrictedRegoLimits.MAX_SOURCE_DEPTH,
          "restricted-rego-v1 source depth must not exceed %s",
          RestrictedRegoLimits.MAX_SOURCE_DEPTH);
      nodeCount += countNodes(expression);
      arrayElementCount += countArrayElements(expression);
    }
    Preconditions.checkArgument(
        nodeCount <= RestrictedRegoLimits.MAX_AST_NODES,
        "restricted-rego-v1 AST must not exceed %s nodes",
        RestrictedRegoLimits.MAX_AST_NODES);
    Preconditions.checkArgument(
        arrayElementCount <= RestrictedRegoLimits.MAX_TOTAL_ARRAY_ELEMENTS,
        "restricted-rego-v1 literal arrays must not contain more than %s total elements",
        RestrictedRegoLimits.MAX_TOTAL_ARRAY_ELEMENTS);
  }

  private static List<CanonicalExpression> expressions(RestrictedRegoProgram program) {
    List<CanonicalExpression> expressions = new ArrayList<>();
    if (program instanceof RowFilter) {
      RowFilter filter = (RowFilter) program;
      for (FilterBranch branch : filter.branches()) {
        expressions.add(branch.result());
        expressions.add(branch.condition());
      }
      expressions.add(filter.fallback());
    } else {
      ColumnMask mask = (ColumnMask) program;
      for (MaskBranch branch : mask.branches()) {
        expressions.add(branch.condition());
      }
    }
    return expressions;
  }

  private static int countNodes(CanonicalExpression expression) {
    if (expression instanceof Comparison) {
      Comparison comparison = (Comparison) expression;
      return 1 + countNodes(comparison.left()) + countNodes(comparison.right());
    }
    if (expression instanceof Not) {
      return 1 + countNodes(((Not) expression).operand());
    }
    if (expression instanceof LogicalExpression) {
      int count = 1;
      for (CanonicalExpression child : ((LogicalExpression) expression).operands()) {
        count += countNodes(child);
      }
      return count;
    }
    return 1;
  }

  private static int countArrayElements(CanonicalExpression expression) {
    if (expression instanceof Comparison) {
      Comparison comparison = (Comparison) expression;
      return countArrayElements(comparison.left()) + countArrayElements(comparison.right());
    }
    if (expression instanceof Not) {
      return countArrayElements(((Not) expression).operand());
    }
    if (expression instanceof LogicalExpression) {
      int count = 0;
      for (CanonicalExpression child : ((LogicalExpression) expression).operands()) {
        count += countArrayElements(child);
      }
      return count;
    }
    if (expression instanceof LiteralArray) {
      return ((LiteralArray) expression).values().size();
    }
    return 0;
  }
}
