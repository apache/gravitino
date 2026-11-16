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
package org.apache.gravitino.catalog.clickhouse.converter;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.clickhouse.operations.ClickHouseClusterUtils;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

public class ClickHouseColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {

  protected static final String NOW = "now";
  protected static final Expression DEFAULT_VALUE_OF_NOW = FunctionExpression.of(NOW);

  // ClickHouse Date stores days since 1970-01-01 as UInt16, so its range is fixed.
  private static final LocalDate MIN_DATE = LocalDate.of(1970, 1, 1);
  private static final LocalDate MAX_DATE = LocalDate.of(2149, 6, 6);
  private static final Pattern NON_FINITE_PATTERN = Pattern.compile("(?i)-?(nan|inf|infinity)");
  private static final Pattern UUID_PATTERN =
      Pattern.compile(
          "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

  /**
   * Checks that a literal default value fits the column type. ClickHouse does not validate DEFAULT
   * literals when a table is created or altered, and silently wraps or clamps out-of-range values
   * on insert. Only the following column types are checked:
   *
   * <ul>
   *   <li>Integer types (signed and unsigned): the value, truncated toward zero as ClickHouse does,
   *       must be within the type range.
   *   <li>Decimal: the value, with excess fractional digits discarded as ClickHouse does, must fit
   *       the precision.
   *   <li>Date: the value must be a {@code yyyy-MM-dd} date between 1970-01-01 and 2149-06-06.
   *   <li>UUID: the value must be a valid UUID string.
   *   <li>Float and Double: the value must be a number, NaN or infinity. The range is not checked.
   * </ul>
   *
   * <p>Other column types such as DateTime, and non-literal or NULL defaults, are not checked.
   *
   * @param columnType the Gravitino type of the column
   * @param defaultValue the default value of the column
   * @throws IllegalArgumentException if the literal value is not valid for the column type
   */
  public void checkDefaultValue(Type columnType, Expression defaultValue) {
    if (!(defaultValue instanceof Literal<?> literal) || literal.value() == null) {
      return;
    }
    String value = literal.value().toString();
    // Validate the same value that fromGravitino writes, which drops caller-supplied quotes.
    if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
      value = ClickHouseClusterUtils.unescapeSingleQuotes(value.substring(1, value.length() - 1));
    }
    checkLiteralFitsType(columnType, value);
  }

  public String fromGravitino(Expression defaultValue) {
    if (DEFAULT_VALUE_NOT_SET.equals(defaultValue)) {
      return null;
    }

    if (defaultValue instanceof FunctionExpression functionExpression) {
      return String.format("(%s)", functionExpression);
    }

    if (defaultValue instanceof Literal<?> literal) {
      Type type = literal.dataType();
      if (defaultValue.equals(Literals.NULL)) {
        return NULL;
      } else if (type instanceof Type.NumericType) {
        // Numeric values are written without quotes, so anything other than a number must be
        // rejected. ClickHouse also accepts NaN and infinity literals for floating-point columns.
        String value = literal.value().toString().trim();
        // Drop caller-supplied quotes the same way checkDefaultValue does, then write the number.
        if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
          value = value.substring(1, value.length() - 1).trim();
        }
        return NON_FINITE_PATTERN.matcher(value).matches() ? value : validateNumericLiteral(value);
      } else {
        Object value = literal.value();
        Preconditions.checkArgument(
            value != null, "Null default literal value is not supported for ClickHouse.");
        if (value instanceof LocalDateTime) {
          value = ((LocalDateTime) value).format(DATE_TIME_FORMATTER);
        } else if (value instanceof String str
            && str.length() >= 2
            && str.startsWith("'")
            && str.endsWith("'")) {
          // The API caller may pass quoted string literals (e.g. "'active'"), but DDL generation
          // is responsible for adding the outer quotes.
          value = ClickHouseClusterUtils.unescapeSingleQuotes(str.substring(1, str.length() - 1));
        }

        // ClickHouse treats backslash as an escape character in string literals, so escape it
        // first to keep the value unchanged on round-trip.
        String escaped = value.toString().replace("\\", "\\\\");
        return String.format("'%s'", ClickHouseClusterUtils.escapeSingleQuotes(escaped));
      }
    }

    throw new IllegalArgumentException("Not a supported column default value: " + defaultValue);
  }

  @Override
  public Expression toGravitino(
      JdbcTypeConverter.JdbcTypeBean type,
      String columnDefaultValue,
      boolean isExpression,
      boolean nullable) {
    if (columnDefaultValue == null || columnDefaultValue.isEmpty()) {
      return nullable ? Literals.NULL : DEFAULT_VALUE_NOT_SET;
    }

    // Handle Nullable type Nullable(type) -> type
    String realType = TypeUtils.stripNullable(type.getTypeName());

    if (realType.startsWith("Decimal(")) {
      realType = "Decimal";
    }

    if (realType.startsWith("FixedString(")) {
      realType = "FixedString";
    }

    if (nullable) {
      if (columnDefaultValue.equals("NULL")) {
        return Literals.NULL;
      }
    }

    // TODO clickhouse has bug which isExpression is false when is really expression
    if (isExpression) {
      if (columnDefaultValue.equals(NOW)) {
        return DEFAULT_VALUE_OF_NOW;
      }
      // The parsing of ClickHouse expressions is complex, so we are not currently undertaking the
      // parsing.
      return UnparsedExpression.of(columnDefaultValue);
    }

    // need exclude begin and end "'"
    boolean quoted =
        columnDefaultValue.length() >= 2
            && columnDefaultValue.startsWith("'")
            && columnDefaultValue.endsWith("'");
    String rawValue =
        quoted
            ? columnDefaultValue.substring(1, columnDefaultValue.length() - 1)
            : columnDefaultValue;
    String reallyValue = quoted ? unescapeStringLiteral(rawValue) : rawValue;

    try {
      switch (realType) {
        case ClickHouseTypeConverter.INT8:
          return Literals.byteLiteral(Byte.valueOf(reallyValue));
        case ClickHouseTypeConverter.UINT8:
          return Literals.unsignedByteLiteral(Short.valueOf(reallyValue));
        case ClickHouseTypeConverter.INT16:
          return Literals.shortLiteral(Short.valueOf(reallyValue));
        case ClickHouseTypeConverter.UINT16:
          return Literals.unsignedShortLiteral(Integer.valueOf(reallyValue));
        case ClickHouseTypeConverter.INT32:
          return Literals.integerLiteral(Integer.valueOf(reallyValue));
        case ClickHouseTypeConverter.UINT32:
          return Literals.unsignedIntegerLiteral(Long.valueOf(reallyValue));
        case ClickHouseTypeConverter.INT64:
          return Literals.longLiteral(Long.valueOf(reallyValue));
        case ClickHouseTypeConverter.UINT64:
          return Literals.unsignedLongLiteral(Decimal.of(reallyValue));
        case ClickHouseTypeConverter.FLOAT32:
          return Literals.floatLiteral(Float.valueOf(reallyValue));
        case ClickHouseTypeConverter.FLOAT64:
          return Literals.doubleLiteral(Double.valueOf(reallyValue));
        case ClickHouseTypeConverter.DECIMAL:
          if (reallyValue.equals("0.")) {
            reallyValue = "0.0";
          }
          return Literals.decimalLiteral(
              Decimal.of(reallyValue, type.getColumnSize(), type.getScale()));
        case ClickHouseTypeConverter.DATE:
          if (StringUtils.isBlank(realType)) {
            throw new IllegalArgumentException("Can't convert blank default value for DATE type.");
          }
          return Literals.dateLiteral(LocalDate.parse(reallyValue, DATE_FORMATTER));
        case ClickHouseTypeConverter.DATETIME:
          return CURRENT_TIMESTAMP.equals(reallyValue)
              ? DEFAULT_VALUE_OF_CURRENT_TIMESTAMP
              : Literals.timestampLiteral(LocalDateTime.parse(reallyValue, DATE_TIME_FORMATTER));
        case ClickHouseTypeConverter.STRING:
          return Literals.of(reallyValue, Types.StringType.get());
        case ClickHouseTypeConverter.FIXEDSTRING:
          return Literals.of(reallyValue, Types.FixedCharType.of(type.getColumnSize()));
        default:
          return UnparsedExpression.of(rawValue);
      }
    } catch (Exception ex) {
      return UnparsedExpression.of(rawValue);
    }
  }

  private static void checkLiteralFitsType(Type columnType, String value) {
    try {
      if (columnType instanceof Types.ByteType byteType) {
        checkIntegralRange(value, 8, byteType.signed(), columnType);
      } else if (columnType instanceof Types.ShortType shortType) {
        checkIntegralRange(value, 16, shortType.signed(), columnType);
      } else if (columnType instanceof Types.IntegerType integerType) {
        checkIntegralRange(value, 32, integerType.signed(), columnType);
      } else if (columnType instanceof Types.LongType longType) {
        checkIntegralRange(value, 64, longType.signed(), columnType);
      } else if (columnType instanceof Types.FloatType || columnType instanceof Types.DoubleType) {
        if (!NON_FINITE_PATTERN.matcher(value).matches()) {
          new BigDecimal(value);
        }
      } else if (columnType instanceof Types.DecimalType decimalType) {
        // ClickHouse discards excess fractional digits, so the value fits as long as its integer
        // part has at most (precision - scale) digits.
        BigDecimal limit =
            BigDecimal.ONE.scaleByPowerOfTen(decimalType.precision() - decimalType.scale());
        Preconditions.checkArgument(
            new BigDecimal(value).abs().compareTo(limit) < 0,
            "Default value %s is out of range for column type %s",
            value,
            columnType.simpleString());
      } else if (columnType instanceof Types.DateType) {
        LocalDate date = LocalDate.parse(value);
        Preconditions.checkArgument(
            !date.isBefore(MIN_DATE) && !date.isAfter(MAX_DATE),
            "Default value %s is out of range for column type %s, ClickHouse Date supports [%s, %s]",
            value,
            columnType.simpleString(),
            MIN_DATE,
            MAX_DATE);
      } else if (columnType instanceof Types.UUIDType) {
        Preconditions.checkArgument(
            UUID_PATTERN.matcher(value).matches(), "Default value %s is not a valid UUID", value);
      }
    } catch (NumberFormatException | DateTimeParseException e) {
      throw new IllegalArgumentException(
          String.format(
              "Default value %s is not a valid value of column type %s",
              value, columnType.simpleString()),
          e);
    }
  }

  private static void checkIntegralRange(String value, int bits, boolean signed, Type columnType) {
    BigInteger min = signed ? BigInteger.ONE.shiftLeft(bits - 1).negate() : BigInteger.ZERO;
    BigInteger max =
        signed
            ? BigInteger.ONE.shiftLeft(bits - 1).subtract(BigInteger.ONE)
            : BigInteger.ONE.shiftLeft(bits).subtract(BigInteger.ONE);
    // ClickHouse converts a fractional or exponent literal to an integer by truncating toward
    // zero (1.9 becomes 1, 1e3 becomes 1000), so the value fits when min - 1 < value < max + 1.
    BigDecimal number = new BigDecimal(value);
    Preconditions.checkArgument(
        number.compareTo(new BigDecimal(min.subtract(BigInteger.ONE))) > 0
            && number.compareTo(new BigDecimal(max.add(BigInteger.ONE))) < 0,
        "Default value %s is out of range for column type %s, valid range is [%s, %s]",
        value,
        columnType.simpleString(),
        min,
        max);
  }

  /**
   * Unescapes the content of a ClickHouse single-quoted string literal. ClickHouse reports string
   * defaults with backslash escapes (for example {@code 'a\'b'}), and also accepts a doubled single
   * quote.
   */
  private static String unescapeStringLiteral(String text) {
    if (text.indexOf('\\') < 0 && !text.contains("''")) {
      return text;
    }
    StringBuilder result = new StringBuilder(text.length());
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == '\\' && i + 1 < text.length()) {
        char next = text.charAt(++i);
        switch (next) {
          case 'n':
            result.append('\n');
            break;
          case 't':
            result.append('\t');
            break;
          case 'r':
            result.append('\r');
            break;
          case '0':
            result.append('\0');
            break;
          case 'b':
            result.append('\b');
            break;
          case 'f':
            result.append('\f');
            break;
          case 'a':
            result.append('\u0007');
            break;
          case 'v':
            result.append('\u000B');
            break;
          case 'x':
            if (i + 2 < text.length()
                && isHexDigit(text.charAt(i + 1))
                && isHexDigit(text.charAt(i + 2))) {
              result.append((char) Integer.parseInt(text.substring(i + 1, i + 3), 16));
              i += 2;
            } else {
              result.append(next);
            }
            break;
          default:
            result.append(next);
        }
      } else if (c == '\'' && i + 1 < text.length() && text.charAt(i + 1) == '\'') {
        result.append('\'');
        i++;
      } else {
        result.append(c);
      }
    }
    return result.toString();
  }

  private static boolean isHexDigit(char c) {
    return Character.digit(c, 16) >= 0;
  }
}
