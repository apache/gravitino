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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import org.apache.gravitino.iceberg.service.restriction.SqlRestrictionBinding.Term;
import org.apache.gravitino.policy.sql.Between;
import org.apache.gravitino.policy.sql.CaseBranch;
import org.apache.gravitino.policy.sql.CaseExpression;
import org.apache.gravitino.policy.sql.Comparison;
import org.apache.gravitino.policy.sql.ComparisonOperator;
import org.apache.gravitino.policy.sql.FunctionCall;
import org.apache.gravitino.policy.sql.In;
import org.apache.gravitino.policy.sql.InputReference;
import org.apache.gravitino.policy.sql.IsNull;
import org.apache.gravitino.policy.sql.Like;
import org.apache.gravitino.policy.sql.Literal;
import org.apache.gravitino.policy.sql.LiteralType;
import org.apache.gravitino.policy.sql.LogicalExpression;
import org.apache.gravitino.policy.sql.LogicalOperator;
import org.apache.gravitino.policy.sql.Not;
import org.apache.gravitino.policy.sql.SqlExpression;
import org.apache.gravitino.policy.sql.SqlFunction;
import org.apache.gravitino.policy.sql.SqlType;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Lowers SQL three-valued predicates to disjoint Iceberg true/false predicates. */
final class SqlRestrictionPredicates {
  private final SqlRestrictionBinding binding;
  private int work;

  SqlRestrictionPredicates(SqlRestrictionBinding binding) {
    this.binding = binding;
  }

  Truth resolve(SqlExpression expression) {
    step();
    if (expression instanceof Not) {
      return resolve(((Not) expression).operand()).not();
    }
    if (expression instanceof LogicalExpression) {
      LogicalExpression logical = (LogicalExpression) expression;
      List<Expression> yes = new ArrayList<>();
      List<Expression> no = new ArrayList<>();
      for (SqlExpression operand : logical.operands()) {
        Truth child = resolve(operand);
        yes.add(child.yes);
        no.add(child.no);
      }
      boolean and = logical.operator() == LogicalOperator.AND;
      return new Truth(
          SqlRestrictionCanonicalizer.combine(and, yes),
          SqlRestrictionCanonicalizer.combine(!and, no));
    }
    if (expression instanceof CaseExpression) {
      return resolveCase((CaseExpression) expression, this::resolve);
    }
    if (expression instanceof Comparison) {
      Comparison comparison = (Comparison) expression;
      return comparison(comparison.operator(), comparison.left(), comparison.right());
    }
    if (expression instanceof IsNull) {
      IsNull test = (IsNull) expression;
      Truth result = isNull(test.operand());
      return test.negated() ? result.not() : result;
    }
    if (expression instanceof In) {
      In in = (In) expression;
      return in(in.operand(), in.values());
    }
    if (expression instanceof Between) {
      Between between = (Between) expression;
      return comparison(ComparisonOperator.GTE, between.operand(), between.lower())
          .and(comparison(ComparisonOperator.LTE, between.operand(), between.upper()));
    }
    if (expression instanceof Like) {
      Like like = (Like) expression;
      return like(like.operand(), like.pattern().value());
    }
    if (expression instanceof FunctionCall
        && ((FunctionCall) expression).function() == SqlFunction.IS_NAN) {
      return isNaN(((FunctionCall) expression).arguments().get(0));
    }
    if (expression instanceof InputReference) {
      Term term = binding.term(expression);
      return new Truth(
          binding.bind(term, Expressions.equal(term.expression, true)),
          binding.bind(term, Expressions.equal(term.expression, false)));
    }
    Literal literal = binding.constant(expression);
    Object value = SqlRestrictionLiterals.value(literal, Types.BooleanType.get());
    return constant((Boolean) value);
  }

  Literal constantScalar(SqlExpression expression) {
    if (expression instanceof CaseExpression) {
      CaseExpression choice = (CaseExpression) expression;
      for (CaseBranch branch : choice.branches()) {
        Truth condition =
            choice.selector() == null
                ? resolve(branch.condition())
                : comparison(ComparisonOperator.EQ, choice.selector(), branch.condition());
        if (!condition.constant()) {
          throw SqlRestrictionBinding.unsupported("a row-dependent scalar CASE");
        }
        if (condition.yes.op() == Expression.Operation.TRUE) {
          return constantScalar(branch.result());
        }
      }
      return choice.fallback() == null
          ? SqlRestrictionBinding.nullLiteral()
          : constantScalar(choice.fallback());
    }
    if (expression instanceof Literal
        || expression instanceof FunctionCall
            && ((FunctionCall) expression).function() != SqlFunction.IS_NAN) {
      return binding.constant(expression);
    }
    Truth value = resolve(expression);
    if (!value.constant()) {
      throw SqlRestrictionBinding.unsupported("a row-dependent scalar result");
    }
    if (value.yes.op() == Expression.Operation.TRUE) {
      return SqlRestrictionBinding.bool(true);
    }
    if (value.no.op() == Expression.Operation.TRUE) {
      return SqlRestrictionBinding.bool(false);
    }
    return SqlRestrictionBinding.nullLiteral();
  }

  Truth equal(SqlExpression left, SqlExpression right) {
    return comparison(ComparisonOperator.EQ, left, right);
  }

  private void step() {
    if (++work > 16384) {
      throw new IllegalArgumentException("SQL restriction compilation exceeds work limit");
    }
  }

  private Truth comparison(ComparisonOperator operator, SqlExpression left, SqlExpression right) {
    step();
    if (left instanceof CaseExpression) {
      return resolveCase((CaseExpression) left, value -> comparison(operator, value, right));
    }
    if (right instanceof CaseExpression) {
      return resolveCase((CaseExpression) right, value -> comparison(operator, left, value));
    }
    if (booleanShape(left) || booleanShape(right)) {
      Truth a = resolve(left);
      Truth b = resolve(right);
      Truth equality =
          new Truth(
              Expressions.or(Expressions.and(a.yes, b.yes), Expressions.and(a.no, b.no)),
              Expressions.or(Expressions.and(a.yes, b.no), Expressions.and(a.no, b.yes)));
      return operator == ComparisonOperator.EQ ? equality : equality.not();
    }
    boolean leftRow = rowDependent(left);
    boolean rightRow = rowDependent(right);
    if (leftRow && rightRow) {
      throw SqlRestrictionBinding.unsupported("column-to-column comparisons");
    }
    if (leftRow || rightRow) {
      Term term = binding.term(leftRow ? left : right);
      Literal literal = constantScalar(leftRow ? right : left);
      Object value = term.value(literal);
      if (value == null) {
        return constant(null);
      }
      ComparisonOperator normalized = leftRow ? operator : invert(operator);
      Expression predicate = binding.bind(term, predicate(normalized, term, value));
      Expression present = binding.bind(term, Expressions.notNull(term.field.name()));
      // Iceberg EQ is false for null, while several negated/ordering operations include null.
      Expression yes =
          normalized == ComparisonOperator.EQ ? predicate : Expressions.and(present, predicate);
      return new Truth(yes, Expressions.and(present, Expressions.not(predicate)));
    }
    Literal a = constantScalar(left);
    Literal b = constantScalar(right);
    Type type =
        a.type() == null
            ? b.type() == null ? null : SqlRestrictionLiterals.type(b.type())
            : SqlRestrictionLiterals.type(a.type());
    if (type != null
        && (type.typeId() == Type.TypeID.BINARY
            || type.typeId() == Type.TypeID.BOOLEAN && operator.ordered())) {
      throw new IllegalArgumentException("Unsupported value comparison for " + type);
    }
    Object av =
        type == null
            ? a.literalType() == LiteralType.NULL ? null : new BigDecimal(a.value())
            : SqlRestrictionLiterals.value(a, type);
    Object bv =
        type == null
            ? b.literalType() == LiteralType.NULL ? null : new BigDecimal(b.value())
            : SqlRestrictionLiterals.value(b, type);
    if (av == null || bv == null) {
      return constant(null);
    }
    int result = SqlRestrictionLiterals.compare(av, bv);
    switch (operator) {
      case EQ:
        return constant(result == 0);
      case NEQ:
        return constant(result != 0);
      case LT:
        return constant(result < 0);
      case LTE:
        return constant(result <= 0);
      case GT:
        return constant(result > 0);
      case GTE:
        return constant(result >= 0);
      default:
        throw new IllegalArgumentException("Unsupported comparison operator");
    }
  }

  private Truth isNull(SqlExpression operand) {
    if (operand instanceof CaseExpression) {
      return resolveCase((CaseExpression) operand, this::isNull);
    }
    if (rowDependent(operand)) {
      Term term = binding.term(operand);
      return new Truth(
          binding.bind(term, Expressions.isNull(term.field.name())),
          binding.bind(term, Expressions.notNull(term.field.name())));
    }
    return constant(constantScalar(operand).literalType() == LiteralType.NULL);
  }

  private Truth in(SqlExpression operand, List<SqlExpression> elements) {
    if (operand instanceof CaseExpression) {
      return resolveCase((CaseExpression) operand, value -> in(value, elements));
    }
    List<Truth> equalities = new ArrayList<>();
    if (!rowDependent(operand)) {
      Truth result = constant(false);
      for (SqlExpression element : elements) {
        equalities.add(comparison(ComparisonOperator.EQ, operand, element));
      }
      for (Truth equality : equalities) {
        result = result.or(equality);
      }
      return result;
    }
    Term term = binding.term(operand);
    Map<String, Object> values = new TreeMap<>();
    boolean containsNull = false;
    for (SqlExpression element : elements) {
      Object value = term.value((Literal) element);
      if (value == null) {
        containsNull = true;
      } else {
        values.put(SqlRestrictionLiterals.canonicalValue(value), value);
      }
    }
    if (values.isEmpty()) {
      return constant(null);
    }
    Expression yes = binding.bind(term, Expressions.in(term.expression, values.values()));
    Expression no =
        containsNull
            ? Expressions.alwaysFalse()
            : Expressions.and(
                binding.bind(term, Expressions.notNull(term.field.name())),
                binding.bind(term, Expressions.notIn(term.expression, values.values())));
    return new Truth(yes, no);
  }

  private Truth like(SqlExpression operand, String pattern) {
    if (operand instanceof CaseExpression) {
      return resolveCase((CaseExpression) operand, value -> like(value, pattern));
    }
    String prefix = pattern.substring(0, pattern.length() - 1);
    if (!rowDependent(operand)) {
      Object value = SqlRestrictionLiterals.value(constantScalar(operand), Types.StringType.get());
      return value == null ? constant(null) : constant(((String) value).startsWith(prefix));
    }
    Term term = binding.term(operand);
    Expression raw = binding.bind(term, Expressions.startsWith(term.field.name(), prefix));
    Expression present = binding.bind(term, Expressions.notNull(term.field.name()));
    return new Truth(Expressions.and(present, raw), Expressions.and(present, Expressions.not(raw)));
  }

  private Truth isNaN(SqlExpression operand) {
    if (operand instanceof CaseExpression) {
      return resolveCase((CaseExpression) operand, this::isNaN);
    }
    if (!rowDependent(operand)) {
      Literal literal = constantScalar(operand);
      if (literal.literalType() == LiteralType.NULL) {
        return constant(null);
      }
      Object value =
          SqlRestrictionLiterals.value(literal, SqlRestrictionLiterals.type(literal.type()));
      return constant(
          value instanceof Float ? Float.isNaN((Float) value) : Double.isNaN((Double) value));
    }
    Term term = binding.term(operand);
    Expression present = binding.bind(term, Expressions.notNull(term.field.name()));
    return new Truth(
        binding.bind(term, Expressions.isNaN(term.expression)),
        Expressions.and(present, binding.bind(term, Expressions.notNaN(term.expression))));
  }

  private Truth resolveCase(CaseExpression choice, Function<SqlExpression, Truth> result) {
    List<Truth> conditions = new ArrayList<>();
    List<Truth> values = new ArrayList<>();
    Truth fallback = null;
    for (CaseBranch branch : choice.branches()) {
      Truth condition =
          choice.selector() == null
              ? resolve(branch.condition())
              : comparison(ComparisonOperator.EQ, choice.selector(), branch.condition());
      if (condition.yes.op() == Expression.Operation.FALSE) {
        continue;
      }
      Truth value = result.apply(branch.result());
      if (condition.yes.op() == Expression.Operation.TRUE) {
        fallback = value;
        break;
      }
      conditions.add(condition);
      values.add(value);
    }
    if (fallback == null) {
      fallback =
          choice.fallback() == null
              ? result.apply(SqlRestrictionBinding.nullLiteral())
              : result.apply(choice.fallback());
    }
    for (int i = conditions.size() - 1; i >= 0; i--) {
      Expression selected = conditions.get(i).yes;
      Truth value = values.get(i);
      fallback =
          new Truth(
              Expressions.or(
                  Expressions.and(selected, value.yes),
                  Expressions.and(Expressions.not(selected), fallback.yes)),
              Expressions.or(
                  Expressions.and(selected, value.no),
                  Expressions.and(Expressions.not(selected), fallback.no)));
    }
    return fallback;
  }

  private static boolean rowDependent(SqlExpression expression) {
    if (expression instanceof InputReference) {
      return true;
    }
    for (SqlExpression child : expression.children()) {
      if (rowDependent(child)) {
        return true;
      }
    }
    return false;
  }

  private static boolean booleanShape(SqlExpression expression) {
    if (expression instanceof Literal) {
      return ((Literal) expression).literalType() == LiteralType.BOOLEAN;
    }
    if (expression instanceof InputReference) {
      return SqlType.BOOLEAN.equals(expression.type());
    }
    if (expression instanceof FunctionCall) {
      SqlFunction function = ((FunctionCall) expression).function();
      return function == SqlFunction.IS_GROUP_MEMBER || function == SqlFunction.IS_NAN;
    }
    return expression instanceof LogicalExpression
        || expression instanceof Not
        || expression instanceof Comparison
        || expression instanceof In
        || expression instanceof Between
        || expression instanceof Like
        || expression instanceof IsNull;
  }

  private static Expression predicate(ComparisonOperator operator, Term term, Object value) {
    switch (operator) {
      case EQ:
        return Expressions.equal(term.expression, value);
      case NEQ:
        return Expressions.notEqual(term.expression, value);
      case LT:
        return Expressions.lessThan(term.expression, value);
      case LTE:
        return Expressions.lessThanOrEqual(term.expression, value);
      case GT:
        return Expressions.greaterThan(term.expression, value);
      case GTE:
        return Expressions.greaterThanOrEqual(term.expression, value);
      default:
        throw new IllegalArgumentException("Unsupported comparison operator");
    }
  }

  private static ComparisonOperator invert(ComparisonOperator operator) {
    switch (operator) {
      case LT:
        return ComparisonOperator.GT;
      case LTE:
        return ComparisonOperator.GTE;
      case GT:
        return ComparisonOperator.LT;
      case GTE:
        return ComparisonOperator.LTE;
      default:
        return operator;
    }
  }

  private static Truth constant(Boolean value) {
    return new Truth(
        Boolean.TRUE.equals(value) ? Expressions.alwaysTrue() : Expressions.alwaysFalse(),
        Boolean.FALSE.equals(value) ? Expressions.alwaysTrue() : Expressions.alwaysFalse());
  }

  static final class Truth {
    final Expression yes;
    final Expression no;

    Truth(Expression yes, Expression no) {
      this.yes = yes;
      this.no = no;
    }

    boolean constant() {
      return (yes.op() == Expression.Operation.TRUE || yes.op() == Expression.Operation.FALSE)
          && (no.op() == Expression.Operation.TRUE || no.op() == Expression.Operation.FALSE);
    }

    Truth not() {
      return new Truth(no, yes);
    }

    Truth and(Truth other) {
      return new Truth(Expressions.and(yes, other.yes), Expressions.or(no, other.no));
    }

    Truth or(Truth other) {
      return new Truth(Expressions.or(yes, other.yes), Expressions.and(no, other.no));
    }
  }
}
