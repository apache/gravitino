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
package org.apache.gravitino.catalog.jdbc.converter;

import static org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils.escapeSqlLiteral;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

public class JdbcColumnDefaultValueConverter {

  /** The DDL rendering of the current timestamp default value function. */
  public static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";

  private static final Pattern FUNCTION_NAME =
      Pattern.compile("[\\p{L}_][\\p{L}\\p{N}_$]*+(?:\\.[\\p{L}_][\\p{L}\\p{N}_$]*+){0,2}");

  protected static final String NULL = "NULL";
  protected static final DateTimeFormatter DATE_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("yyyy-MM-dd HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
          .toFormatter();
  protected static final DateTimeFormatter DATE_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd");

  protected static final DateTimeFormatter TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
          .toFormatter();

  public String fromGravitino(Expression defaultValue) {
    if (DEFAULT_VALUE_NOT_SET.equals(defaultValue)) {
      return null;
    }

    if (defaultValue instanceof FunctionExpression) {
      FunctionExpression functionExpression = (FunctionExpression) defaultValue;
      String name = functionExpression.functionName();
      Preconditions.checkArgument(
          name != null && FUNCTION_NAME.matcher(name).matches(),
          "JDBC column default function name must be a plain or qualified identifier: %s",
          name);
      Expression[] arguments = functionExpression.arguments();
      Preconditions.checkArgument(
          arguments != null, "JDBC default function arguments must not be null");
      if (name.equalsIgnoreCase(CURRENT_TIMESTAMP)) {
        Preconditions.checkArgument(
            arguments.length == 0, "CURRENT_TIMESTAMP default does not accept arguments");
        return CURRENT_TIMESTAMP;
      }
      String[] sqlArguments = new String[arguments.length];
      for (int i = 0; i < arguments.length; i++) {
        Preconditions.checkArgument(
            arguments[i] instanceof Literal || arguments[i] instanceof FunctionExpression,
            "Unsupported JDBC column default function argument at index %s",
            i);
        sqlArguments[i] = fromGravitino(arguments[i]);
        Preconditions.checkArgument(
            sqlArguments[i] != null,
            "JDBC column default function argument at index %s must have a value",
            i);
      }
      return "(" + name + "(" + String.join(", ", sqlArguments) + "))";
    }

    if (defaultValue instanceof Literal) {
      Literal<?> literal = (Literal<?>) defaultValue;
      Type type = literal.dataType();
      if (defaultValue.equals(Literals.NULL)) {
        return NULL;
      } else if (type instanceof Type.NumericType) {
        String value = literal.value().toString();
        // It seems that literals.value().toString() can be an empty string for numeric types
        // in some cases like `alter table t modify column `id` int null default '';`, in such
        // case value is an empty string, we should wrap it with single quotes to avoid SQL error.
        if (StringUtils.isBlank(value)) {
          value = "'%s'".formatted(value);
        } else {
          value = validateNumericLiteral(value);
        }
        return value;
      } else if (type instanceof Types.BinaryType && literal.value() instanceof byte[]) {
        return binaryLiteral((byte[]) literal.value());
      } else if (type instanceof Types.BinaryType
          && literal.value() instanceof String
          && ((String) literal.value()).startsWith("0x")) {
        byte[] value;
        try {
          value =
              BaseEncoding.base16()
                  .lowerCase()
                  .decode(((String) literal.value()).substring(2).toLowerCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(
              "Invalid binary column default value: use 0x followed by hexadecimal byte pairs", e);
        }
        return binaryLiteral(value);
      } else if (type instanceof Types.TimestampType) {
        // java.time omits seconds for whole-minute values, but MySQL requires them.
        String value =
            literal
                .value()
                .toString()
                .replace("T", " ")
                .replaceFirst("^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2})(?=$|Z|[+-])", "$1:00");
        if (((Types.TimestampType) type).hasTimeZone() && value.endsWith("Z")) {
          // MySQL accepts numeric offsets but rejects the equivalent ISO UTC suffix.
          value = value.substring(0, value.length() - 1) + "+00:00";
        }
        return quoteStringLiteral(value);
      } else {
        return quoteStringLiteral(String.valueOf(literal.value()));
      }
    }

    throw new IllegalArgumentException("Not a supported column default value: " + defaultValue);
  }

  public Expression toGravitino(
      JdbcTypeConverter.JdbcTypeBean columnType,
      String columnDefaultValue,
      boolean isExpression,
      boolean nullable) {
    return DEFAULT_VALUE_NOT_SET;
  }

  /**
   * Renders a value as a quoted SQL string literal for a column default. The default escapes
   * backslashes and doubles single quotes, as MySQL-compatible dialects require. Dialects with
   * different string literal rules override this method.
   *
   * @param value the unquoted literal value
   * @return the quoted and escaped SQL string literal
   */
  protected String quoteStringLiteral(String value) {
    return "'" + escapeSqlLiteral(value, '\'') + "'";
  }

  /**
   * Renders binary bytes as a SQL literal. Dialects can override their binary literal syntax.
   *
   * @param value the bytes to preserve
   * @return a SQL expression containing the binary value
   */
  protected String binaryLiteral(byte[] value) {
    return "X'" + BaseEncoding.base16().lowerCase().encode(value) + "'";
  }

  /**
   * Checks that a numeric default value is a plain number, so it can be written into the DDL
   * without quotes.
   *
   * @param value the numeric default value text
   * @return the trimmed value
   * @throws IllegalArgumentException if the value is not a valid number
   */
  protected static String validateNumericLiteral(String value) {
    String trimmed = value.trim();
    try {
      new BigDecimal(trimmed);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Invalid numeric column default value: %s", value), e);
    }
    return trimmed;
  }
}
