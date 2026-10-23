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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** An n-ary Boolean conjunction or disjunction. */
final class LogicalExpression implements CanonicalExpression {
  private final LogicalOperator operator;
  private final List<CanonicalExpression> operands;

  LogicalExpression(LogicalOperator operator, List<CanonicalExpression> operands) {
    this.operator = operator;
    this.operands =
        operands == null ? null : Collections.unmodifiableList(new ArrayList<>(operands));
  }

  static CanonicalExpression of(LogicalOperator operator, List<CanonicalExpression> operands) {
    if (operands == null) {
      return new LogicalExpression(operator, null);
    }

    List<CanonicalExpression> flattened = new ArrayList<>();
    for (CanonicalExpression operand : operands) {
      if (operand instanceof LogicalExpression
          && ((LogicalExpression) operand).operator() == operator) {
        flattened.addAll(((LogicalExpression) operand).operands());
      } else {
        flattened.add(operand);
      }
    }
    return flattened.size() == 1 ? flattened.get(0) : new LogicalExpression(operator, flattened);
  }

  LogicalOperator operator() {
    return operator;
  }

  List<CanonicalExpression> operands() {
    return operands;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(operator != null, "logical expression operator cannot be null");
    Preconditions.checkArgument(
        operands != null && operands.size() >= 2,
        "%s requires at least two operands",
        operator.sourceToken());
    for (CanonicalExpression child : operands) {
      Preconditions.checkArgument(
          child != null, "%s operand cannot be null", operator.sourceToken());
      child.validate();
      Preconditions.checkArgument(
          ExpressionValidator.isPredicate(child),
          "%s operands must be boolean predicates",
          operator.sourceToken());
    }
  }

  @Override
  public int depth() {
    int childDepth = 0;
    if (operands != null) {
      for (CanonicalExpression child : operands) {
        if (child != null) {
          childDepth = Math.max(childDepth, child.depth());
        }
      }
    }
    return 1 + childDepth;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LogicalExpression)) {
      return false;
    }
    LogicalExpression that = (LogicalExpression) other;
    return operator == that.operator && Objects.equals(operands, that.operands);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operator, operands);
  }

  @Override
  public String toString() {
    return "LogicalExpression{" + "operator=" + operator + ", operands=" + operands + '}';
  }
}
