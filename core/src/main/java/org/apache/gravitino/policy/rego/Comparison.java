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
import java.util.Objects;

/** A binary comparison operation. */
final class Comparison implements CanonicalExpression {
  private final ComparisonOperator operator;
  private final CanonicalExpression left;
  private final CanonicalExpression right;

  Comparison(ComparisonOperator operator, CanonicalExpression left, CanonicalExpression right) {
    this.operator = operator;
    this.left = left;
    this.right = right;
  }

  ComparisonOperator operator() {
    return operator;
  }

  CanonicalExpression left() {
    return left;
  }

  CanonicalExpression right() {
    return right;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(operator != null, "comparison operator cannot be null");
    Preconditions.checkArgument(
        left != null && right != null, "comparison operands cannot be null");
    left.validate();
    right.validate();
    try {
      ExpressionValidator.validateComparison(operator, left, right);
    } catch (IllegalArgumentException exception) {
      throw new IllegalArgumentException(
          String.format(
              "%s; operator '%s' with left=%s and right=%s",
              exception.getMessage(),
              operator.sourceToken(),
              operandKind(left),
              operandKind(right)),
          exception);
    }
  }

  private static String operandKind(CanonicalExpression expression) {
    if (expression instanceof Literal) {
      return "literal(" + ((Literal) expression).literalType() + ")";
    }
    if (expression instanceof LiteralArray) {
      return "literal-array(" + ((LiteralArray) expression).elementType() + ")";
    }
    if (expression instanceof Column) {
      return "column";
    }
    if (expression instanceof SessionUser) {
      return "session-user";
    }
    if (expression instanceof GroupMembership) {
      return "group-membership";
    }
    if (expression instanceof Not) {
      return "not";
    }
    if (expression instanceof LogicalExpression) {
      return "logical(" + ((LogicalExpression) expression).operator().sourceToken() + ")";
    }
    return expression.getClass().getSimpleName();
  }

  @Override
  public int depth() {
    return 1;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Comparison)) {
      return false;
    }
    Comparison that = (Comparison) other;
    return operator == that.operator
        && Objects.equals(left, that.left)
        && Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operator, left, right);
  }

  @Override
  public String toString() {
    return "Comparison{" + "operator=" + operator + ", left=" + left + ", right=" + right + '}';
  }
}
