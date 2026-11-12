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
package org.apache.gravitino.iceberg.service.restriction;

import javax.annotation.Nullable;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

/**
 * A SQL Boolean result represented by two disjoint Iceberg predicates.
 *
 * <p>The true expression selects rows where the SQL result is TRUE; the false expression selects
 * rows where it is FALSE. Rows where the result is UNKNOWN match neither expression. For example,
 * when lowering {@code region = 'US'}, a null region matches neither predicate. The false
 * expression therefore cannot be obtained by simply negating the true expression.
 *
 * <p>Keeping both predicates preserves UNKNOWN through SQL NOT, AND, OR and CASE evaluation.
 */
final class SqlPredicateResult {
  private final Expression trueExpression;
  private final Expression falseExpression;

  /** Creates a result from predicates whose matching row sets are disjoint. */
  SqlPredicateResult(Expression trueExpression, Expression falseExpression) {
    this.trueExpression = trueExpression;
    this.falseExpression = falseExpression;
  }

  /** Creates a constant SQL result; null represents UNKNOWN and matches neither predicate. */
  static SqlPredicateResult constant(@Nullable Boolean value) {
    return new SqlPredicateResult(
        Boolean.TRUE.equals(value) ? Expressions.alwaysTrue() : Expressions.alwaysFalse(),
        Boolean.FALSE.equals(value) ? Expressions.alwaysTrue() : Expressions.alwaysFalse());
  }

  /** Returns the predicate selecting rows where the SQL result is TRUE. */
  Expression trueExpression() {
    return trueExpression;
  }

  /** Returns the predicate selecting rows where the SQL result is FALSE. */
  Expression falseExpression() {
    return falseExpression;
  }

  /** Returns whether both predicates are constant, including the UNKNOWN result. */
  boolean isConstant() {
    return (trueExpression.op() == Expression.Operation.TRUE
            || trueExpression.op() == Expression.Operation.FALSE)
        && (falseExpression.op() == Expression.Operation.TRUE
            || falseExpression.op() == Expression.Operation.FALSE);
  }

  /** Applies SQL NOT by swapping TRUE and FALSE, leaving UNKNOWN unmatched. */
  SqlPredicateResult not() {
    return new SqlPredicateResult(falseExpression, trueExpression);
  }

  /** Applies SQL AND: both operands must be TRUE, while either FALSE operand makes FALSE. */
  SqlPredicateResult and(SqlPredicateResult other) {
    return new SqlPredicateResult(
        Expressions.and(trueExpression, other.trueExpression),
        Expressions.or(falseExpression, other.falseExpression));
  }

  /**
   * Applies SQL OR: either TRUE operand makes TRUE, while both operands must be FALSE for FALSE.
   */
  SqlPredicateResult or(SqlPredicateResult other) {
    return new SqlPredicateResult(
        Expressions.or(trueExpression, other.trueExpression),
        Expressions.and(falseExpression, other.falseExpression));
  }
}
