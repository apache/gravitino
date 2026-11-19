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
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Pins SQL three-valued logic independently of AST conversion and schema binding. */
class TestSqlPredicateResult {
  private static final Types.StructType ROW_TYPE =
      Types.StructType.of(Types.NestedField.optional(1, "flag", Types.BooleanType.get()));

  @Test
  void testConstantResults() {
    for (Boolean value : new Boolean[] {true, false, null}) {
      assertConstant(value, SqlPredicateResult.constant(value));
    }
  }

  @Test
  void testNotPreservesUnknown() {
    assertConstant(false, SqlPredicateResult.constant(true).not());
    assertConstant(true, SqlPredicateResult.constant(false).not());
    assertConstant(null, SqlPredicateResult.constant(null).not());
  }

  @Test
  void testAndOrTruthTables() {
    // Columns: left operand, right operand, expected AND result, expected OR result.
    Boolean[][] cases = {
      {true, true, true, true},
      {true, false, false, true},
      {true, null, null, true},
      {false, true, false, true},
      {false, false, false, false},
      {false, null, false, null},
      {null, true, null, true},
      {null, false, false, null},
      {null, null, null, null}
    };
    for (Boolean[] test : cases) {
      SqlPredicateResult left = SqlPredicateResult.constant(test[0]);
      SqlPredicateResult right = SqlPredicateResult.constant(test[1]);
      assertConstant(test[2], left.and(right));
      assertConstant(test[3], left.or(right));
    }
  }

  @Test
  void testRowPredicatesDistinguishUnknownFromFalse() {
    Expression whenTrue = Expressions.equal("flag", true);
    Expression whenFalse = Expressions.equal("flag", false);
    SqlPredicateResult result = new SqlPredicateResult(whenTrue, whenFalse);
    Assertions.assertFalse(result.isConstant());
    Assertions.assertFalse(
        new SqlPredicateResult(Expressions.alwaysFalse(), whenFalse).isConstant());
    Assertions.assertFalse(
        new SqlPredicateResult(whenTrue, Expressions.alwaysFalse()).isConstant());

    for (Boolean value : new Boolean[] {true, false, null}) {
      StructLike row = row(value);
      Assertions.assertEquals(Boolean.TRUE.equals(value), matches(result.trueExpression(), row));
      Assertions.assertEquals(Boolean.FALSE.equals(value), matches(result.falseExpression(), row));
      Assertions.assertEquals(
          Boolean.FALSE.equals(value), matches(result.not().trueExpression(), row));
      Assertions.assertEquals(
          Boolean.TRUE.equals(value), matches(result.not().falseExpression(), row));
    }
  }

  private static void assertConstant(@Nullable Boolean expected, SqlPredicateResult result) {
    Assertions.assertTrue(result.isConstant());
    Assertions.assertEquals(
        Boolean.TRUE.equals(expected) ? Expression.Operation.TRUE : Expression.Operation.FALSE,
        result.trueExpression().op());
    Assertions.assertEquals(
        Boolean.FALSE.equals(expected) ? Expression.Operation.TRUE : Expression.Operation.FALSE,
        result.falseExpression().op());
  }

  private static boolean matches(Expression expression, StructLike row) {
    BoundPredicate<?> predicate = (BoundPredicate<?>) Binder.bind(ROW_TYPE, expression, true);
    return predicate.test(row);
  }

  private static StructLike row(@Nullable Boolean value) {
    return new StructLike() {
      @Override
      public int size() {
        return 1;
      }

      @Override
      public <T> T get(int pos, Class<T> type) {
        return type.cast(value);
      }

      @Override
      public <T> void set(int pos, T value) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
