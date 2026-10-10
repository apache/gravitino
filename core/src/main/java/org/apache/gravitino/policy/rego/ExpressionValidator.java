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
import org.apache.gravitino.policy.rego.CanonicalExpression.Column;
import org.apache.gravitino.policy.rego.CanonicalExpression.Comparison;
import org.apache.gravitino.policy.rego.CanonicalExpression.ComparisonOperator;
import org.apache.gravitino.policy.rego.CanonicalExpression.GroupMembership;
import org.apache.gravitino.policy.rego.CanonicalExpression.Literal;
import org.apache.gravitino.policy.rego.CanonicalExpression.LiteralArray;
import org.apache.gravitino.policy.rego.CanonicalExpression.LiteralType;
import org.apache.gravitino.policy.rego.CanonicalExpression.LogicalExpression;
import org.apache.gravitino.policy.rego.CanonicalExpression.Not;
import org.apache.gravitino.policy.rego.CanonicalExpression.SessionUser;

/** Semantic validation helpers for unresolved restricted Rego expressions. */
final class ExpressionValidator {
  private ExpressionValidator() {}

  static boolean isPredicate(CanonicalExpression expression) {
    if (expression instanceof Comparison
        || expression instanceof Not
        || expression instanceof LogicalExpression
        || expression instanceof GroupMembership) {
      return true;
    }
    return expression instanceof Literal
        && ((Literal) expression).literalType() == LiteralType.BOOLEAN;
  }

  static boolean isContextOnly(CanonicalExpression expression) {
    if (expression instanceof Column) {
      return false;
    }
    if (expression instanceof Comparison) {
      Comparison comparison = (Comparison) expression;
      return isContextOnly(comparison.left()) && isContextOnly(comparison.right());
    }
    if (expression instanceof Not) {
      return isContextOnly(((Not) expression).operand());
    }
    if (expression instanceof LogicalExpression) {
      for (CanonicalExpression child : ((LogicalExpression) expression).operands()) {
        if (!isContextOnly(child)) {
          return false;
        }
      }
    }
    return true;
  }

  static void validateComparison(
      ComparisonOperator operator, CanonicalExpression left, CanonicalExpression right) {
    if (operator == ComparisonOperator.IN) {
      Preconditions.checkArgument(
          right instanceof LiteralArray, "right operand of in must be a literal array");
      Preconditions.checkArgument(
          left instanceof Column || left instanceof SessionUser,
          "left operand of in must be col(...) or session_user()");
      if (left instanceof SessionUser) {
        LiteralArray array = (LiteralArray) right;
        Preconditions.checkArgument(
            array.elementType() == LiteralType.STRING,
            "session_user() can only be tested against a string array");
      }
      return;
    }

    Preconditions.checkArgument(
        !(left instanceof LiteralArray) && !(right instanceof LiteralArray),
        "%s does not support array operands",
        operator.sourceToken());

    boolean equality = operator == ComparisonOperator.EQ || operator == ComparisonOperator.NEQ;
    if (isColumnLiteralPair(left, right)) {
      Literal literal = left instanceof Literal ? (Literal) left : (Literal) right;
      Preconditions.checkArgument(
          literal.literalType() != LiteralType.NULL || equality, "null supports only == and !=");
      Preconditions.checkArgument(
          literal.literalType() != LiteralType.BOOLEAN || equality,
          "boolean literal supports only == and !=");
      return;
    }

    if (isColumnSessionUserPair(left, right)) {
      Preconditions.checkArgument(equality, "session_user() supports only == and !=");
      return;
    }

    if (isSessionUserStringPair(left, right)) {
      Preconditions.checkArgument(equality, "session_user() supports only == and !=");
      return;
    }

    throw new IllegalArgumentException(
        String.format("Unsupported operands for %s in restricted-rego-v1", operator.sourceToken()));
  }

  static void validateUnicodeScalars(String value, String description) {
    for (int index = 0; index < value.length(); index++) {
      char current = value.charAt(index);
      Preconditions.checkArgument(current != '\0', "%s cannot contain NUL", description);
      if (Character.isHighSurrogate(current)) {
        Preconditions.checkArgument(
            index + 1 < value.length() && Character.isLowSurrogate(value.charAt(index + 1)),
            "%s cannot contain an isolated surrogate",
            description);
        index++;
      } else {
        Preconditions.checkArgument(
            !Character.isLowSurrogate(current),
            "%s cannot contain an isolated surrogate",
            description);
      }
    }
  }

  private static boolean isColumnLiteralPair(CanonicalExpression left, CanonicalExpression right) {
    return (left instanceof Column && right instanceof Literal)
        || (right instanceof Column && left instanceof Literal);
  }

  private static boolean isColumnSessionUserPair(
      CanonicalExpression left, CanonicalExpression right) {
    return (left instanceof Column && right instanceof SessionUser)
        || (right instanceof Column && left instanceof SessionUser);
  }

  private static boolean isSessionUserStringPair(
      CanonicalExpression left, CanonicalExpression right) {
    if (left instanceof SessionUser && right instanceof Literal) {
      return ((Literal) right).literalType() == LiteralType.STRING;
    }
    if (right instanceof SessionUser && left instanceof Literal) {
      return ((Literal) left).literalType() == LiteralType.STRING;
    }
    return false;
  }
}
