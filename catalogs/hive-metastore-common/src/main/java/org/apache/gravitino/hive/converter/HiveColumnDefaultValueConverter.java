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
package org.apache.gravitino.hive.converter;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts column default values between Gravitino {@link Expression}s and the SQL expression
 * strings stored by the Hive Metastore in {@code SQLDefaultConstraint}. Literals and zero-argument
 * functions are modeled; any other expression is carried verbatim as an {@link UnparsedExpression}
 * so it round-trips unchanged.
 */
public class HiveColumnDefaultValueConverter {

  private static final Logger LOG = LoggerFactory.getLogger(HiveColumnDefaultValueConverter.class);
  private static final String NULL = "NULL";
  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("yyyy-MM-dd HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .toFormatter();

  private HiveColumnDefaultValueConverter() {}

  /**
   * Renders a Gravitino default value as a Hive SQL expression string.
   *
   * @param defaultValue The Gravitino default value expression.
   * @return The Hive SQL expression, or {@code null} when no default value is set.
   */
  public static String fromGravitino(Expression defaultValue) {
    if (defaultValue == null || DEFAULT_VALUE_NOT_SET.equals(defaultValue)) {
      return null;
    }

    if (defaultValue instanceof UnparsedExpression) {
      return ((UnparsedExpression) defaultValue).unparsedExpression();
    }

    if (defaultValue instanceof FunctionExpression) {
      FunctionExpression function = (FunctionExpression) defaultValue;
      if (function.arguments().length == 0) {
        // Hive requires parentheses for functions such as CURRENT_TIMESTAMP(); the trailing "()"
        // is also what toGravitino keys on to recognize a function
        return function.functionName().toUpperCase() + "()";
      }
      throw new IllegalArgumentException(
          "Hive catalog does not support function default value with arguments: " + defaultValue);
    }

    if (defaultValue instanceof Literal) {
      Literal<?> literal = (Literal<?>) defaultValue;
      Type type = literal.dataType();
      if (literal.value() == null || type.name() == Type.Name.NULL) {
        return NULL;
      }
      if (type instanceof Type.NumericType || type instanceof Types.BooleanType) {
        return literal.value().toString();
      }
      if (type instanceof Types.TimestampType && literal.value() instanceof LocalDateTime) {
        return quote(((LocalDateTime) literal.value()).format(DATE_TIME_FORMATTER));
      }
      return quote(literal.value().toString());
    }

    throw new IllegalArgumentException("Not a supported column default value: " + defaultValue);
  }

  /**
   * Parses a Hive SQL default value expression into a Gravitino {@link Expression}.
   *
   * @param type The Gravitino type of the column that owns the default value.
   * @param defaultValue The Hive SQL expression string.
   * @return The Gravitino default value expression.
   */
  public static Expression toGravitino(Type type, String defaultValue) {
    if (defaultValue == null) {
      return DEFAULT_VALUE_NOT_SET;
    }
    String value = defaultValue.trim();
    if (value.equalsIgnoreCase(NULL)) {
      return Literals.NULL;
    }
    if (value.endsWith("()") && value.length() > 2) {
      return FunctionExpression.of(value.substring(0, value.length() - 2).toLowerCase());
    }
    boolean quoted = value.length() >= 2 && value.startsWith("'") && value.endsWith("'");
    if (quoted) {
      value = unquote(value);
    }

    Type.Name typeName = type.name();
    try {
      switch (typeName) {
        case BOOLEAN:
          // Boolean.valueOf() maps anything but "true" to false, so only accept the two literals
          if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            return Literals.booleanLiteral(Boolean.valueOf(value));
          }
          break;
        case BYTE:
          return Literals.byteLiteral(Byte.valueOf(value));
        case SHORT:
          return Literals.shortLiteral(Short.valueOf(value));
        case INTEGER:
          return Literals.integerLiteral(Integer.valueOf(value));
        case LONG:
          return Literals.longLiteral(Long.valueOf(value));
        case FLOAT:
          return Literals.floatLiteral(Float.valueOf(value));
        case DOUBLE:
          return Literals.doubleLiteral(Double.valueOf(value));
        case DECIMAL:
          Types.DecimalType decimalType = (Types.DecimalType) type;
          return Literals.decimalLiteral(
              Decimal.of(value, decimalType.precision(), decimalType.scale()));
        case DATE:
          return Literals.dateLiteral(LocalDate.parse(value));
        case TIMESTAMP:
          return Literals.timestampLiteral(LocalDateTime.parse(value, DATE_TIME_FORMATTER));
        case STRING:
        case VARCHAR:
        case FIXEDCHAR:
          // An unquoted value is an expression such as CAST(...) rather than a string literal
          if (quoted) {
            return Literals.of(value, type);
          }
          break;
        default:
          break;
      }
    } catch (IllegalArgumentException | DateTimeParseException e) {
      // The metastore stores arbitrary SQL text; whatever cannot be modeled as a literal of the
      // column type is deliberately kept verbatim instead of failing the table load.
      LOG.debug("Cannot parse Hive default value '{}' for type {}", defaultValue, type, e);
    }
    return UnparsedExpression.of(defaultValue);
  }

  private static String quote(String value) {
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
  }

  private static String unquote(String quoted) {
    return quoted.substring(1, quoted.length() - 1).replace("\\'", "'").replace("\\\\", "\\");
  }
}
