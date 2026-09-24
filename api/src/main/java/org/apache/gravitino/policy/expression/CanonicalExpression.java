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
import java.util.Locale;
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

    private final String value;

    Operator(String value) {
      this.value = value;
    }

    /**
     * Parses a source or canonical operator value.
     *
     * @param value operator value
     * @return parsed operator
     */
    public static Operator fromValue(String value) {
      Preconditions.checkArgument(value != null && !value.isEmpty(), "operator cannot be empty");
      switch (value) {
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
        default:
          for (Operator operator : values()) {
            if (operator.value.equals(value)) {
              return operator;
            }
          }
          throw new IllegalArgumentException("Unsupported restricted-rego-v1 operator: " + value);
      }
    }

    /**
     * Returns the canonical operator value.
     *
     * @return canonical operator value
     */
    public String value() {
      return value;
    }
  }

  /** Source literal types supported by {@code restricted-rego-v1}. */
  enum LiteralType {
    /** String literal. */
    STRING("string"),
    /** Exact base-10 numeric literal. */
    NUMBER("number"),
    /** Boolean literal. */
    BOOLEAN("boolean"),
    /** Null literal. */
    NULL("null");

    private final String value;

    LiteralType(String value) {
      this.value = value;
    }

    /**
     * Parses a literal type value.
     *
     * @param value literal type value
     * @return parsed literal type
     */
    public static LiteralType fromValue(String value) {
      Preconditions.checkArgument(
          value != null && !value.isEmpty(), "literal type cannot be empty");
      return LiteralType.valueOf(value.toUpperCase(Locale.ROOT));
    }

    /**
     * Returns the literal type value.
     *
     * @return literal type value
     */
    public String value() {
      return value;
    }
  }

  /** An allowlisted operation node. */
  final class Operation implements CanonicalExpression {
    private final Operator op;

    private final CanonicalExpression left;

    private final CanonicalExpression right;

    private final CanonicalExpression operand;

    private final List<CanonicalExpression> operands;

    private Operation() {
      this(null, null, null, null, null);
    }

    Operation(
        Operator op,
        CanonicalExpression left,
        CanonicalExpression right,
        CanonicalExpression operand,
        List<CanonicalExpression> operands) {
      this.op = op;
      this.left = left;
      this.right = right;
      this.operand = operand;
      this.operands =
          operands == null ? null : Collections.unmodifiableList(new ArrayList<>(operands));
    }

    /**
     * Returns the operation.
     *
     * @return operation
     */
    public Operator op() {
      return op;
    }

    /**
     * Returns the left comparison operand.
     *
     * @return left operand, or {@code null} for non-comparison operations
     */
    public CanonicalExpression left() {
      return left;
    }

    /**
     * Returns the right comparison operand.
     *
     * @return right operand, or {@code null} for non-comparison operations
     */
    public CanonicalExpression right() {
      return right;
    }

    /**
     * Returns the unary operand.
     *
     * @return unary operand, or {@code null} for other operations
     */
    public CanonicalExpression operand() {
      return operand;
    }

    /**
     * Returns the conjunction or disjunction operands.
     *
     * @return immutable operands, or {@code null} for other operations
     */
    public List<CanonicalExpression> operands() {
      return operands;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(op != null, "operation cannot be null");
      switch (op) {
        case EQ:
        case NEQ:
        case LT:
        case LTE:
        case GT:
        case GTE:
        case IN:
          Preconditions.checkArgument(
              left != null && right != null, "%s requires left and right operands", op.value());
          Preconditions.checkArgument(
              operand == null && operands == null,
              "%s cannot contain logical operands",
              op.value());
          left.validate();
          right.validate();
          ExpressionValidation.validateComparison(op, left, right);
          break;
        case NOT:
          Preconditions.checkArgument(operand != null, "not requires an operand");
          Preconditions.checkArgument(
              left == null && right == null && operands == null,
              "not cannot contain comparison or boolean operands");
          operand.validate();
          Preconditions.checkArgument(
              ExpressionValidation.isPredicate(operand), "not operand must be a boolean predicate");
          break;
        case AND:
        case OR:
          Preconditions.checkArgument(
              operands != null && operands.size() >= 2,
              "%s requires at least two operands",
              op.value());
          Preconditions.checkArgument(
              left == null && right == null && operand == null,
              "%s cannot contain comparison or unary operands",
              op.value());
          for (CanonicalExpression child : operands) {
            Preconditions.checkArgument(child != null, "%s operand cannot be null", op.value());
            child.validate();
            Preconditions.checkArgument(
                ExpressionValidation.isPredicate(child),
                "%s operands must be boolean predicates",
                op.value());
          }
          break;
        default:
          throw new IllegalArgumentException("Unsupported restricted-rego-v1 operator: " + op);
      }
    }

    @Override
    public int depth() {
      if (op == Operator.NOT) {
        return operand == null ? 1 : 1 + operand.depth();
      }
      if (op == Operator.AND || op == Operator.OR) {
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
      return 1;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Operation)) {
        return false;
      }
      Operation that = (Operation) other;
      return op == that.op
          && Objects.equals(left, that.left)
          && Objects.equals(right, that.right)
          && Objects.equals(operand, that.operand)
          && Objects.equals(operands, that.operands);
    }

    @Override
    public int hashCode() {
      return Objects.hash(op, left, right, operand, operands);
    }

    @Override
    public String toString() {
      return "Operation{"
          + "op="
          + op
          + ", left="
          + left
          + ", right="
          + right
          + ", operand="
          + operand
          + ", operands="
          + operands
          + '}';
    }
  }

  /** A symbolic top-level column reference. */
  final class Column implements CanonicalExpression {
    private final String type;

    private final String name;

    private Column() {
      this(null);
    }

    Column(String name) {
      this.type = "column";
      this.name = name;
    }

    /**
     * Returns the node type.
     *
     * @return {@code column}
     */
    public String type() {
      return type;
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
      Preconditions.checkArgument("column".equals(type), "column node type must be 'column'");
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
      return Objects.equals(type, that.type) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, name);
    }

    @Override
    public String toString() {
      return "Column{" + "name='" + name + '\'' + '}';
    }
  }

  /** A symbolic reference to the trusted effective session user. */
  final class SessionUser implements CanonicalExpression {
    private final String type;

    /** Creates a session-user reference. */
    SessionUser() {
      this.type = "session-user";
    }

    /**
     * Returns the node type.
     *
     * @return {@code session-user}
     */
    public String type() {
      return type;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          "session-user".equals(type), "session user node type must be 'session-user'");
    }

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
    private final String type;

    private final String group;

    private GroupMembership() {
      this(null);
    }

    GroupMembership(String group) {
      this.type = "group-membership";
      this.group = group;
    }

    /**
     * Returns the node type.
     *
     * @return {@code group-membership}
     */
    public String type() {
      return type;
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
      Preconditions.checkArgument(
          "group-membership".equals(type), "group membership node type must be 'group-membership'");
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
      return Objects.equals(type, that.type) && Objects.equals(group, that.group);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, group);
    }

    @Override
    public String toString() {
      return "GroupMembership{" + "group='" + group + '\'' + '}';
    }
  }

  /** A decoded source literal. */
  final class Literal implements CanonicalExpression {
    private final String type;

    private final LiteralType literalType;

    private final Object value;

    private final boolean negativeZero;

    private Literal() {
      this(null, null, false);
    }

    Literal(LiteralType literalType, Object value) {
      this(literalType, value, false);
    }

    Literal(LiteralType literalType, Object value, boolean negativeZero) {
      this.type = "literal";
      this.literalType = literalType;
      this.value = value;
      this.negativeZero = negativeZero;
    }

    /**
     * Returns the node type.
     *
     * @return {@code literal}
     */
    public String type() {
      return type;
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
      Preconditions.checkArgument("literal".equals(type), "literal node type must be 'literal'");
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
      if (!Objects.equals(type, that.type)
          || literalType != that.literalType
          || negativeZero != that.negativeZero) {
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
      return Objects.hash(type, literalType, normalizedValue, negativeZero);
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
    private final String type;

    private final LiteralType elementType;

    private final List<Literal> values;

    private LiteralArray() {
      this(null, null);
    }

    LiteralArray(LiteralType elementType, List<Literal> values) {
      this.type = "array";
      this.elementType = elementType;
      this.values = values == null ? null : Collections.unmodifiableList(new ArrayList<>(values));
    }

    /**
     * Returns the node type.
     *
     * @return {@code array}
     */
    public String type() {
      return type;
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
      Preconditions.checkArgument("array".equals(type), "array node type must be 'array'");
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
      return Objects.equals(type, that.type)
          && elementType == that.elementType
          && Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, elementType, values);
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
    if (expression instanceof CanonicalExpression.Operation
        || expression instanceof CanonicalExpression.GroupMembership) {
      return true;
    }
    return expression instanceof CanonicalExpression.Literal
        && ((CanonicalExpression.Literal) expression).literalType()
            == CanonicalExpression.LiteralType.BOOLEAN;
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
        operator.value());

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
        String.format("Unsupported operands for %s in restricted-rego-v1", operator.value()));
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
