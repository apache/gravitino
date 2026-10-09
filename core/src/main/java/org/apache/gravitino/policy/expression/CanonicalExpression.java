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

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A node in an unresolved {@code restricted-rego-v1} expression tree.
 *
 * <p>The tree contains only the source constructs allowlisted by the read-restriction contract.
 * Column and context references remain symbolic until the effective-policy resolver binds them to a
 * table schema and request context.
 */
public interface CanonicalExpression {

  /**
   * Validates this expression node and all of its children.
   *
   * @throws IllegalArgumentException if the expression is malformed or outside the allowlisted
   *     source profile
   */
  void validate() throws IllegalArgumentException;

  /**
   * Returns the source operation depth defined by {@code restricted-rego-v1}.
   *
   * @return expression depth
   */
  int depth();

  /** Operators supported by {@code restricted-rego-v1}. */
  enum Operator {
    /** Equality comparison. */
    EQ("eq"),
    /** Inequality comparison. */
    NEQ("neq"),
    /** Less-than comparison. */
    LT("lt"),
    /** Less-than-or-equal comparison. */
    LTE("lte"),
    /** Greater-than comparison. */
    GT("gt"),
    /** Greater-than-or-equal comparison. */
    GTE("gte"),
    /** Literal-array membership comparison. */
    IN("in"),
    /** Boolean conjunction. */
    AND("and"),
    /** Boolean disjunction. */
    OR("or"),
    /** Boolean negation. */
    NOT("not");

    private final String canonicalName;

    Operator(String canonicalName) {
      this.canonicalName = canonicalName;
    }

    /**
     * Parses an operator token from restricted Rego source.
     *
     * @param token source operator token
     * @return parsed operator
     */
    public static Operator fromSourceToken(String token) {
      Preconditions.checkArgument(token != null && !token.isEmpty(), "operator cannot be empty");
      switch (token) {
        case "==":
          return EQ;
        case "!=":
          return NEQ;
        case "<":
          return LT;
        case "<=":
          return LTE;
        case ">":
          return GT;
        case ">=":
          return GTE;
        case "in":
          return IN;
        default:
          throw new IllegalArgumentException("Unsupported restricted-rego-v1 operator: " + token);
      }
    }

    /**
     * Returns the canonical operator name used by the resolved model.
     *
     * @return canonical operator name
     */
    public String canonicalName() {
      return canonicalName;
    }
  }

  /** Source literal types supported by {@code restricted-rego-v1}. */
  enum LiteralType {
    /** String literal. */
    STRING,
    /** Exact base-10 numeric literal. */
    NUMBER,
    /** Boolean literal. */
    BOOLEAN,
    /** Null literal. */
    NULL
  }

  /** A binary comparison operation. */
  final class Comparison implements CanonicalExpression {
    private final Operator operator;
    private final CanonicalExpression left;
    private final CanonicalExpression right;

    Comparison(Operator operator, CanonicalExpression left, CanonicalExpression right) {
      this.operator = operator;
      this.left = left;
      this.right = right;
    }

    /**
     * Returns the comparison operator.
     *
     * @return comparison operator
     */
    public Operator operator() {
      return operator;
    }

    /**
     * Returns the left operand.
     *
     * @return left operand
     */
    public CanonicalExpression left() {
      return left;
    }

    /**
     * Returns the right operand.
     *
     * @return right operand
     */
    public CanonicalExpression right() {
      return right;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          ExpressionValidation.isComparisonOperator(operator),
          "comparison requires a comparison operator");
      Preconditions.checkArgument(
          left != null && right != null, "comparison operands cannot be null");
      left.validate();
      right.validate();
      ExpressionValidation.validateComparison(operator, left, right);
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

  /** A Boolean negation operation. */
  final class Not implements CanonicalExpression {
    private final CanonicalExpression operand;

    Not(CanonicalExpression operand) {
      this.operand = operand;
    }

    /**
     * Returns the negated predicate.
     *
     * @return negated predicate
     */
    public CanonicalExpression operand() {
      return operand;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(operand != null, "not requires an operand");
      operand.validate();
      Preconditions.checkArgument(
          ExpressionValidation.isPredicate(operand), "not operand must be a boolean predicate");
    }

    @Override
    public int depth() {
      return 1 + (operand == null ? 0 : operand.depth());
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Not && Objects.equals(operand, ((Not) other).operand);
    }

    @Override
    public int hashCode() {
      return Objects.hash(operand);
    }

    @Override
    public String toString() {
      return "Not{" + "operand=" + operand + '}';
    }
  }

  /** An n-ary Boolean conjunction or disjunction. */
  final class Logical implements CanonicalExpression {
    private final Operator operator;
    private final List<CanonicalExpression> operands;

    Logical(Operator operator, List<CanonicalExpression> operands) {
      this.operator = operator;
      this.operands =
          operands == null ? null : Collections.unmodifiableList(new ArrayList<>(operands));
    }

    /**
     * Returns the logical operator.
     *
     * @return {@link Operator#AND} or {@link Operator#OR}
     */
    public Operator operator() {
      return operator;
    }

    /**
     * Returns the logical operands.
     *
     * @return immutable operands
     */
    public List<CanonicalExpression> operands() {
      return operands;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          operator == Operator.AND || operator == Operator.OR,
          "logical expression requires AND or OR");
      Preconditions.checkArgument(
          operands != null && operands.size() >= 2,
          "%s requires at least two operands",
          operator.canonicalName());
      for (CanonicalExpression child : operands) {
        Preconditions.checkArgument(
            child != null, "%s operand cannot be null", operator.canonicalName());
        child.validate();
        Preconditions.checkArgument(
            ExpressionValidation.isPredicate(child),
            "%s operands must be boolean predicates",
            operator.canonicalName());
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
      if (!(other instanceof Logical)) {
        return false;
      }
      Logical that = (Logical) other;
      return operator == that.operator && Objects.equals(operands, that.operands);
    }

    @Override
    public int hashCode() {
      return Objects.hash(operator, operands);
    }

    @Override
    public String toString() {
      return "Logical{" + "operator=" + operator + ", operands=" + operands + '}';
    }
  }

  /** A symbolic top-level column reference. */
  final class Column implements CanonicalExpression {
    private final String name;

    Column(String name) {
      this.name = name;
    }

    /**
     * Returns the exact decoded column name.
     *
     * @return column name
     */
    public String name() {
      return name;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(name != null && !name.isEmpty(), "column name cannot be empty");
      Preconditions.checkArgument(name.indexOf('\0') < 0, "column name cannot contain NUL");
      ExpressionValidation.validateUnicodeScalars(name, "column name");
    }

    @Override
    public int depth() {
      return 0;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Column)) {
        return false;
      }
      Column that = (Column) other;
      return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }

    @Override
    public String toString() {
      return "Column{" + "name='" + name + '\'' + '}';
    }
  }

  /** A symbolic reference to the trusted effective session user. */
  final class SessionUser implements CanonicalExpression {
    /** Creates a session-user reference. */
    SessionUser() {}

    @Override
    public void validate() throws IllegalArgumentException {}

    @Override
    public int depth() {
      return 0;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof SessionUser;
    }

    @Override
    public int hashCode() {
      return SessionUser.class.hashCode();
    }

    @Override
    public String toString() {
      return "SessionUser{}";
    }
  }

  /** A request-context group-membership predicate. */
  final class GroupMembership implements CanonicalExpression {
    private final String group;

    GroupMembership(String group) {
      this.group = group;
    }

    /**
     * Returns the exact decoded group name.
     *
     * @return group name
     */
    public String group() {
      return group;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(group != null && !group.isEmpty(), "group name cannot be empty");
      ExpressionValidation.validateUnicodeScalars(group, "group name");
    }

    @Override
    public int depth() {
      return 1;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof GroupMembership)) {
        return false;
      }
      GroupMembership that = (GroupMembership) other;
      return Objects.equals(group, that.group);
    }

    @Override
    public int hashCode() {
      return Objects.hash(group);
    }

    @Override
    public String toString() {
      return "GroupMembership{" + "group='" + group + '\'' + '}';
    }
  }

  /** A decoded source literal. */
  final class Literal implements CanonicalExpression {
    private final LiteralType literalType;

    private final Object value;

    private final boolean negativeZero;

    Literal(LiteralType literalType, Object value) {
      this(literalType, value, false);
    }

    Literal(LiteralType literalType, Object value, boolean negativeZero) {
      this.literalType = literalType;
      this.value = value;
      this.negativeZero = negativeZero;
    }

    /**
     * Returns the source literal type.
     *
     * @return literal type
     */
    public LiteralType literalType() {
      return literalType;
    }

    /**
     * Returns the decoded value.
     *
     * @return decoded value, or {@code null} for a null literal
     */
    public Object value() {
      return value;
    }

    /**
     * Returns whether the numeric token was a negative zero.
     *
     * @return {@code true} only for a numeric token with a leading minus and mathematical value
     *     zero
     */
    public boolean negativeZero() {
      return negativeZero;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(literalType != null, "literalType cannot be null");
      switch (literalType) {
        case STRING:
          Preconditions.checkArgument(value instanceof String, "string literal must be a string");
          Preconditions.checkArgument(!negativeZero, "string literal cannot be negative zero");
          ExpressionValidation.validateUnicodeScalars((String) value, "string literal");
          break;
        case NUMBER:
          Preconditions.checkArgument(
              value instanceof BigDecimal, "number literal must be an exact decimal");
          Preconditions.checkArgument(
              !negativeZero || ((BigDecimal) value).signum() == 0,
              "negative-zero marker requires a zero value");
          break;
        case BOOLEAN:
          Preconditions.checkArgument(value instanceof Boolean, "boolean literal must be boolean");
          Preconditions.checkArgument(!negativeZero, "boolean literal cannot be negative zero");
          break;
        case NULL:
          Preconditions.checkArgument(value == null, "null literal cannot contain a value");
          Preconditions.checkArgument(!negativeZero, "null literal cannot be negative zero");
          break;
        default:
          throw new IllegalArgumentException("Unsupported literal type: " + literalType);
      }
    }

    @Override
    public int depth() {
      return literalType == LiteralType.BOOLEAN ? 1 : 0;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Literal)) {
        return false;
      }
      Literal that = (Literal) other;
      if (literalType != that.literalType || negativeZero != that.negativeZero) {
        return false;
      }
      if (literalType == LiteralType.NUMBER) {
        return ((BigDecimal) value).compareTo((BigDecimal) that.value) == 0;
      }
      return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      Object normalizedValue =
          literalType == LiteralType.NUMBER ? ((BigDecimal) value).stripTrailingZeros() : value;
      return Objects.hash(literalType, normalizedValue, negativeZero);
    }

    @Override
    public String toString() {
      return "Literal{"
          + "literalType="
          + literalType
          + ", value="
          + value
          + ", negativeZero="
          + negativeZero
          + '}';
    }
  }

  /** A non-empty homogeneous array of non-null source literals. */
  final class LiteralArray implements CanonicalExpression {
    private final LiteralType elementType;

    private final List<Literal> values;

    LiteralArray(LiteralType elementType, List<Literal> values) {
      this.elementType = elementType;
      this.values = values == null ? null : Collections.unmodifiableList(new ArrayList<>(values));
    }

    /**
     * Returns the common element type.
     *
     * @return element type
     */
    public LiteralType elementType() {
      return elementType;
    }

    /**
     * Returns the literal values.
     *
     * @return immutable literal values
     */
    public List<Literal> values() {
      return values;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          elementType != null && elementType != LiteralType.NULL,
          "array element type must be non-null");
      Preconditions.checkArgument(values != null && !values.isEmpty(), "array cannot be empty");
      for (Literal value : values) {
        Preconditions.checkArgument(value != null, "array cannot contain a null node");
        value.validate();
        Preconditions.checkArgument(
            value.literalType() == elementType,
            "array literals must have one homogeneous non-null type");
      }
    }

    @Override
    public int depth() {
      return 0;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof LiteralArray)) {
        return false;
      }
      LiteralArray that = (LiteralArray) other;
      return elementType == that.elementType && Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(elementType, values);
    }

    @Override
    public String toString() {
      return "LiteralArray{" + "elementType=" + elementType + ", values=" + values + '}';
    }
  }
}

final class ExpressionValidation {
  private ExpressionValidation() {}

  static boolean isPredicate(CanonicalExpression expression) {
    if (expression instanceof CanonicalExpression.Comparison
        || expression instanceof CanonicalExpression.Not
        || expression instanceof CanonicalExpression.Logical
        || expression instanceof CanonicalExpression.GroupMembership) {
      return true;
    }
    return expression instanceof CanonicalExpression.Literal
        && ((CanonicalExpression.Literal) expression).literalType()
            == CanonicalExpression.LiteralType.BOOLEAN;
  }

  static boolean isContextOnly(CanonicalExpression expression) {
    if (expression instanceof CanonicalExpression.Column) {
      return false;
    }
    if (expression instanceof CanonicalExpression.Comparison) {
      CanonicalExpression.Comparison comparison = (CanonicalExpression.Comparison) expression;
      return isContextOnly(comparison.left()) && isContextOnly(comparison.right());
    }
    if (expression instanceof CanonicalExpression.Not) {
      return isContextOnly(((CanonicalExpression.Not) expression).operand());
    }
    if (expression instanceof CanonicalExpression.Logical) {
      for (CanonicalExpression child : ((CanonicalExpression.Logical) expression).operands()) {
        if (!isContextOnly(child)) {
          return false;
        }
      }
    }
    return true;
  }

  static boolean isComparisonOperator(CanonicalExpression.Operator operator) {
    return operator == CanonicalExpression.Operator.EQ
        || operator == CanonicalExpression.Operator.NEQ
        || operator == CanonicalExpression.Operator.LT
        || operator == CanonicalExpression.Operator.LTE
        || operator == CanonicalExpression.Operator.GT
        || operator == CanonicalExpression.Operator.GTE
        || operator == CanonicalExpression.Operator.IN;
  }

  static void validateComparison(
      CanonicalExpression.Operator operator, CanonicalExpression left, CanonicalExpression right) {
    if (operator == CanonicalExpression.Operator.IN) {
      Preconditions.checkArgument(
          right instanceof CanonicalExpression.LiteralArray,
          "right operand of in must be a literal array");
      Preconditions.checkArgument(
          left instanceof CanonicalExpression.Column
              || left instanceof CanonicalExpression.SessionUser,
          "left operand of in must be col(...) or session_user()");
      if (left instanceof CanonicalExpression.SessionUser) {
        CanonicalExpression.LiteralArray array = (CanonicalExpression.LiteralArray) right;
        Preconditions.checkArgument(
            array.elementType() == CanonicalExpression.LiteralType.STRING,
            "session_user() can only be tested against a string array");
      }
      return;
    }

    Preconditions.checkArgument(
        !(left instanceof CanonicalExpression.LiteralArray)
            && !(right instanceof CanonicalExpression.LiteralArray),
        "%s does not support array operands",
        operator.canonicalName());

    boolean equality =
        operator == CanonicalExpression.Operator.EQ || operator == CanonicalExpression.Operator.NEQ;
    if (isColumnLiteralPair(left, right)) {
      CanonicalExpression.Literal literal =
          left instanceof CanonicalExpression.Literal
              ? (CanonicalExpression.Literal) left
              : (CanonicalExpression.Literal) right;
      Preconditions.checkArgument(
          literal.literalType() != CanonicalExpression.LiteralType.NULL || equality,
          "null supports only == and !=");
      Preconditions.checkArgument(
          literal.literalType() != CanonicalExpression.LiteralType.BOOLEAN || equality,
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
        String.format(
            "Unsupported operands for %s in restricted-rego-v1", operator.canonicalName()));
  }

  static void validateUnicodeScalars(String value, String description) {
    for (int index = 0; index < value.length(); index++) {
      char current = value.charAt(index);
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
    return (left instanceof CanonicalExpression.Column
            && right instanceof CanonicalExpression.Literal)
        || (right instanceof CanonicalExpression.Column
            && left instanceof CanonicalExpression.Literal);
  }

  private static boolean isColumnSessionUserPair(
      CanonicalExpression left, CanonicalExpression right) {
    return (left instanceof CanonicalExpression.Column
            && right instanceof CanonicalExpression.SessionUser)
        || (right instanceof CanonicalExpression.Column
            && left instanceof CanonicalExpression.SessionUser);
  }

  private static boolean isSessionUserStringPair(
      CanonicalExpression left, CanonicalExpression right) {
    if (left instanceof CanonicalExpression.SessionUser
        && right instanceof CanonicalExpression.Literal) {
      return ((CanonicalExpression.Literal) right).literalType()
          == CanonicalExpression.LiteralType.STRING;
    }
    if (right instanceof CanonicalExpression.SessionUser
        && left instanceof CanonicalExpression.Literal) {
      return ((CanonicalExpression.Literal) left).literalType()
          == CanonicalExpression.LiteralType.STRING;
    }
    return false;
  }
}
