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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/** Checks every source branch without evaluating request context or lowering SQL null semantics. */
final class SqlTypeValidator {
  private final Map<String, SqlType> attributeTypes = new HashMap<>();

  void validate(SqlExpression node, @Nullable SqlType expected) {
    SqlType actual = naturalType(node);
    require(
        expected == null
            || actual == null
            || actual.equals(SqlType.NUMBER) && expected.numeric()
            || expected.equals(actual),
        "expected " + expected + ", found " + actual);
    SqlType hint = expected == null ? actual : expected;
    List<SqlExpression> args = node.children;
    switch (node.operator) {
      case "INPUT":
      case "NULL":
      case "STRING":
      case "BOOLEAN":
        return;
      case "NUMBER":
        validateNumber(node.value, hint);
        return;
      case "TEMPORAL":
        validateTemporal(node);
        return;
      case "AND":
      case "OR":
      case "NOT":
        args.forEach(arg -> validate(arg, SqlType.BOOLEAN));
        return;
      case "IS NULL":
      case "IS NOT NULL":
        validate(args.get(0), null);
        return;
      case "LIKE":
        args.forEach(arg -> validate(arg, SqlType.STRING));
        return;
      case "CASE":
      case "SIMPLE CASE":
        boolean simple = node.operator.equals("SIMPLE CASE");
        List<SqlExpression> matches = new ArrayList<>();
        if (simple) {
          matches.add(args.get(0));
        }
        for (int i = simple ? 1 : 0; i < args.size() - 1; i += 2) {
          if (simple) {
            matches.add(args.get(i));
          } else {
            validate(args.get(i), SqlType.BOOLEAN);
          }
          validate(args.get(i + 1), hint);
        }
        if (simple) {
          validateCompatible(matches, false);
        }
        validate(args.get(args.size() - 1), hint);
        return;
      case "CALL":
        validateCall(node, hint);
        return;
      default:
        validateCompatible(args, node.operator.matches("<|<=|>|>=|BETWEEN"));
    }
  }

  @Nullable
  private SqlType naturalType(SqlExpression node) {
    if (node.type != null) {
      return node.type;
    }
    switch (node.operator) {
      case "NULL":
        return null;
      case "NUMBER":
        return SqlType.NUMBER;
      case "CALL":
        switch (node.value) {
          case "CURRENT_USER":
            return SqlType.STRING;
          case "IS_GROUP_MEMBER":
          case "IS_NAN":
            return SqlType.BOOLEAN;
          case "YEAR":
          case "MONTH":
          case "DAY":
            return SqlType.INTEGER;
          case "USER_ATTRIBUTE":
            return null;
          default:
            return node.children.size() == 1 ? naturalType(node.children.get(0)) : null;
        }
      case "CASE":
      case "SIMPLE CASE":
        List<SqlExpression> results = new ArrayList<>();
        for (int i = node.operator.equals("CASE") ? 1 : 2; i < node.children.size() - 1; i += 2) {
          results.add(node.children.get(i));
        }
        results.add(node.children.get(node.children.size() - 1));
        return commonType(results);
      default:
        return SqlType.BOOLEAN;
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

  private void validateCall(SqlExpression node, @Nullable SqlType hint) {
    List<SqlExpression> args = node.children;
    if (node.value.equals("CURRENT_USER")) {
      require(args.isEmpty(), "CURRENT_USER requires no arguments");
      return;
    }
    require(args.size() == 1, node.value + " requires exactly one argument");
    SqlExpression arg = args.get(0);
    SqlType type = naturalType(arg);
    switch (node.value) {
      case "IS_GROUP_MEMBER":
      case "USER_ATTRIBUTE":
        require(
            arg.operator.equals("STRING") && !arg.value.isEmpty(),
            "context key must be a non-empty string literal");
        if (node.value.equals("USER_ATTRIBUTE") && hint != null && !hint.equals(SqlType.NUMBER)) {
          SqlType previous = attributeTypes.putIfAbsent(arg.value, hint);
          require(
              previous == null || previous.equals(hint),
              "inconsistent user attribute type for " + arg.value);
        }
        return;
      case "IS_NAN":
        require(
            type != null && type.name.matches("FLOAT|DOUBLE"), "IS_NAN requires FLOAT or DOUBLE");
        validate(arg, type);
        return;
      case "YEAR":
      case "MONTH":
      case "DAY":
        require(type != null && type.temporal(), node.value + " requires DATE or TIMESTAMP");
        validate(arg, type);
        return;
      case "MASK_ALPHANUM":
      case "SHOW_FIRST_4":
      case "SHOW_LAST_4":
        require(hint == null || hint.equals(SqlType.STRING), "string mask requires STRING result");
        validate(arg, SqlType.STRING);
        return;
      case "TRUNCATE_TO_YEAR":
      case "TRUNCATE_TO_MONTH":
        type = hint == null ? type : hint;
        require(type != null && type.temporal(), "truncation requires DATE or TIMESTAMP");
        break;
      case "SHA256_GLOBAL":
      case "SHA256_QUERY_LOCAL":
        type = hint == null ? type : hint;
        require(
            type != null && type.name.matches("STRING|INTEGER|LONG|BINARY"),
            "unsupported hash input type");
        break;
      case "MASK_TO_FIXED_VALUE":
        type = hint == null ? type : hint;
        require(
            type != null
                && type.name.matches(
                    "STRING|INTEGER|LONG|FLOAT|DOUBLE|DECIMAL|BOOLEAN|DATE|TIMESTAMP"),
            "unsupported fixed-value input type");
        break;
      case "REPLACE_WITH_NULL":
        type = hint == null ? type : hint;
        require(type != null, "replace_with_null requires a typed scalar");
        break;
      default:
        throw new IllegalArgumentException("Unknown restricted-sql-v1 function: " + node.value);
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
      throw new IllegalArgumentException("Numeric literal does not fit " + type + ": " + source, e);
    }
  }

  private static void validateTemporal(SqlExpression node) {
    String value = node.value;
    SqlType type = node.type;
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
      throw new IllegalArgumentException("Invalid temporal literal: " + value, e);
    }
  }

  private static void require(boolean valid, String message) {
    if (!valid) {
      throw new IllegalArgumentException("Invalid restricted-sql-v1 expression: " + message);
    }
  }
}
