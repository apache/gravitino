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
package org.apache.gravitino.policy.sql;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/** Checks every source branch without evaluating request context or lowering SQL null semantics. */
final class SqlTypeValidator {
  void validate(SqlExpression node, @Nullable SqlType expected) {
    try {
      validateNode(node, expected);
    } catch (InvalidExpressionException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      throw new InvalidExpressionException(node, e);
    }
  }

  private void validateNode(SqlExpression node, @Nullable SqlType expected) {
    SqlType actual = naturalType(node);
    require(
        expected == null
            || actual == null
            || actual.equals(SqlType.NUMBER) && expected.numeric()
            || expected.equals(actual),
        "expected " + expected + ", found " + actual);
    SqlType hint = expected == null ? actual : expected;
    if (node instanceof Literal) {
      Literal literal = (Literal) node;
      if (literal.literalType() == LiteralType.NUMBER) {
        validateNumber(literal.value(), hint);
      }
      if (literal.literalType() == LiteralType.TEMPORAL) {
        validateTemporal(literal);
      }
    } else if (node instanceof InputReference) {
      return;
    } else if (node instanceof LogicalExpression || node instanceof Not) {
      node.children().forEach(arg -> validate(arg, SqlType.BOOLEAN));
    } else if (node instanceof IsNull) {
      validate(((IsNull) node).operand(), null);
    } else if (node instanceof Like) {
      node.children().forEach(arg -> validate(arg, SqlType.STRING));
    } else if (node instanceof CaseExpression) {
      validateCase((CaseExpression) node, hint);
    } else if (node instanceof FunctionCall) {
      validateCall((FunctionCall) node, hint);
    } else if (node instanceof Comparison) {
      validateCompatible(node.children(), ((Comparison) node).operator().ordered());
    } else if (node instanceof In || node instanceof Between) {
      validateCompatible(node.children(), node instanceof Between);
    } else {
      throw new IllegalArgumentException(
          "Unsupported SQL AST node: " + node.getClass().getSimpleName());
    }
  }

  @Nullable
  private SqlType naturalType(SqlExpression node) {
    if (node.type() != null) {
      return node.type();
    }
    if (node instanceof Literal) {
      return ((Literal) node).literalType() == LiteralType.NUMBER ? SqlType.NUMBER : null;
    }
    if (node instanceof FunctionCall) {
      FunctionCall call = (FunctionCall) node;
      switch (call.function()) {
        case CURRENT_USER:
          return SqlType.STRING;
        case IS_GROUP_MEMBER:
        case IS_NAN:
          return SqlType.BOOLEAN;
        case YEAR:
        case MONTH:
        case DAY:
          return SqlType.INTEGER;
        default:
          return call.arguments().size() == 1 ? naturalType(call.arguments().get(0)) : null;
      }
    }
    if (node instanceof CaseExpression) {
      return commonType(((CaseExpression) node).results());
    }
    return SqlType.BOOLEAN;
  }

  private void validateCase(CaseExpression expression, @Nullable SqlType hint) {
    List<SqlExpression> matches = new ArrayList<>();
    if (expression.selector() != null) {
      matches.add(expression.selector());
    }
    for (CaseBranch branch : expression.branches()) {
      if (expression.selector() == null) {
        validate(branch.condition(), SqlType.BOOLEAN);
      } else {
        matches.add(branch.condition());
      }
      validate(branch.result(), hint);
    }
    if (expression.selector() != null) {
      validateCompatible(matches, false);
    }
    if (expression.fallback() != null) {
      validate(expression.fallback(), hint);
    }
  }

  @Nullable
  private SqlType commonType(List<SqlExpression> nodes) {
    SqlType result = null;
    for (SqlExpression node : nodes) {
      SqlType candidate = naturalType(node);
      if (candidate != null && (result == null || result.equals(SqlType.NUMBER))) {
        result = candidate;
      }
    }
    return result;
  }

  private void validateCompatible(List<SqlExpression> args, boolean ordered) {
    SqlType type = commonType(args);
    require(type == null || !type.name.equals("BINARY"), "BINARY value predicates are unsupported");
    require(
        !ordered || type == null || !type.equals(SqlType.BOOLEAN),
        "BOOLEAN ordering is unsupported");
    args.forEach(arg -> validate(arg, type));
  }

  private void validateCall(FunctionCall node, @Nullable SqlType hint) {
    List<SqlExpression> args = node.arguments();
    if (node.function() == SqlFunction.CURRENT_USER) {
      require(args.isEmpty(), "CURRENT_USER requires no arguments");
      return;
    }
    require(args.size() == 1, node.function() + " requires exactly one argument");
    SqlExpression arg = args.get(0);
    SqlType type = naturalType(arg);
    switch (node.function()) {
      case IS_GROUP_MEMBER:
        require(
            arg instanceof Literal
                && ((Literal) arg).literalType() == LiteralType.STRING
                && !((Literal) arg).value().isEmpty(),
            "context key must be a non-empty string literal");
        return;
      case IS_NAN:
        require(
            type != null && type.name.matches("FLOAT|DOUBLE"), "IS_NAN requires FLOAT or DOUBLE");
        validate(arg, type);
        return;
      case YEAR:
      case MONTH:
      case DAY:
        require(type != null && type.temporal(), node.function() + " requires DATE or TIMESTAMP");
        validate(arg, type);
        return;
      case MASK_ALPHANUM:
      case SHOW_FIRST_4:
      case SHOW_LAST_4:
        require(hint == null || hint.equals(SqlType.STRING), "string mask requires STRING result");
        validate(arg, SqlType.STRING);
        return;
      case TRUNCATE_TO_YEAR:
      case TRUNCATE_TO_MONTH:
        type = hint == null ? type : hint;
        require(type != null && type.temporal(), "truncation requires DATE or TIMESTAMP");
        break;
      case SHA256_GLOBAL:
      case SHA256_QUERY_LOCAL:
        type = hint == null ? type : hint;
        require(
            type != null && type.name.matches("STRING|INTEGER|LONG|BINARY"),
            "unsupported hash input type");
        break;
      case MASK_TO_FIXED_VALUE:
        type = hint == null ? type : hint;
        require(
            type != null
                && type.name.matches(
                    "STRING|INTEGER|LONG|FLOAT|DOUBLE|DECIMAL|BOOLEAN|DATE|TIMESTAMP"),
            "unsupported fixed-value input type");
        break;
      case REPLACE_WITH_NULL:
        type = hint == null ? type : hint;
        require(type != null, "replace_with_null requires a typed scalar");
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown restricted-sql-v1 function: " + node.function());
    }
    validate(arg, type);
  }

  private static void validateNumber(String source, @Nullable SqlType type) {
    BigDecimal value = new BigDecimal(source);
    if (type == null || type.equals(SqlType.NUMBER)) {
      return;
    }
    require(type.numeric(), "numeric literal requires a numeric type");
    try {
      switch (type.name) {
        case "INTEGER":
          value.intValueExact();
          break;
        case "LONG":
          value.longValueExact();
          break;
        case "DECIMAL":
          BigDecimal scaled = value.setScale(type.scale);
          require(scaled.precision() <= type.precision, "decimal precision overflow");
          break;
        case "FLOAT":
          require(Float.isFinite(Float.parseFloat(source)), "floating point overflow");
          break;
        case "DOUBLE":
          require(Double.isFinite(Double.parseDouble(source)), "floating point overflow");
          break;
        default:
          break;
      }
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException("Numeric literal does not fit " + type, e);
    }
  }

  private static void validateTemporal(Literal node) {
    String value = node.value();
    SqlType type = node.type();
    require(type != null, "temporal literal requires a type");
    try {
      if (type.name.equals("DATE")) {
        require(value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}"), "invalid DATE literal");
        LocalDate.parse(value);
      } else {
        String clock = "[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,6})?";
        if (type.name.equals("TIME")) {
          require(value.matches(clock), "invalid TIME literal or precision");
          LocalTime.parse(value);
        } else {
          require(
              value.matches(
                  "[0-9]{4}-[0-9]{2}-[0-9]{2} "
                      + clock
                      + (type.withTimeZone ? "(Z|[+-][0-9]{2}:[0-9]{2})" : "")),
              "invalid TIMESTAMP literal, precision or offset");
          if (type.withTimeZone) {
            OffsetDateTime.parse(value.replace(' ', 'T'));
          } else {
            LocalDateTime.parse(value.replace(' ', 'T'));
          }
        }
      }
    } catch (DateTimeException e) {
      throw new IllegalArgumentException("Invalid temporal literal for " + type, e);
    }
  }

  private static void require(boolean valid, String message) {
    if (!valid) {
      throw new IllegalArgumentException("Invalid restricted-sql-v1 expression: " + message);
    }
  }

  /** Retains the innermost failing node without expanding literal payloads in diagnostics. */
  static final class InvalidExpressionException extends IllegalArgumentException {
    private final SqlExpression expression;

    private InvalidExpressionException(SqlExpression expression, IllegalArgumentException cause) {
      super(cause.getMessage(), cause);
      this.expression = expression;
    }

    SqlExpression expression() {
      return expression;
    }
  }
}
