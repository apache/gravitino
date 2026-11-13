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
package org.apache.gravitino.catalog.mysql.converter;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Types;

public class MysqlColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {

  // Only quoted temporal literals may be rendered from an unparsed default.
  private static final Pattern QUOTED_TEMPORAL_LITERAL =
      Pattern.compile(
          "'(?:[0-9]{4}-[0-9]{2}-[0-9]{2}(?: [0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,6})?)?"
              + "|-?[0-9]{2,3}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,6})?)'");

  /**
   * Renders preserved temporal defaults as quoted SQL literals and Boolean defaults as numeric 1 or
   * 0. Gravitino maps BOOLEAN to MySQL BIT, which rejects the quoted 'true' and 'false' strings.
   * The check uses the literal type, so a Boolean default that is loaded from BIT(1) and then
   * re-rendered by MODIFY COLUMN keeps working after a type change to a numeric column. Other
   * defaults use the shared JDBC rendering.
   *
   * @param defaultValue the Gravitino default expression
   * @return the SQL expression, or null when no default is set
   * @throws IllegalArgumentException if a Boolean default is not true, false, 1 or 0
   */
  @Override
  @Nullable
  public String fromGravitino(Expression defaultValue) {
    if (defaultValue instanceof UnparsedExpression) {
      String sql = ((UnparsedExpression) defaultValue).unparsedExpression();
      if (QUOTED_TEMPORAL_LITERAL.matcher(sql).matches()) {
        return sql;
      }
    }
    if (defaultValue instanceof Literal
        && ((Literal<?>) defaultValue).dataType() instanceof Types.BooleanType) {
      String value = String.valueOf(((Literal<?>) defaultValue).value()).trim();
      if ("true".equalsIgnoreCase(value) || "1".equals(value)) {
        return "1";
      }
      if ("false".equalsIgnoreCase(value) || "0".equals(value)) {
        return "0";
      }
      throw new IllegalArgumentException(
          String.format(
              "Invalid boolean column default value: %s; use true, false, 1 or 0", value));
    }
    return super.fromGravitino(defaultValue);
  }

  @Override
  public Expression toGravitino(
      JdbcTypeConverter.JdbcTypeBean type,
      String columnDefaultValue,
      boolean isExpression,
      boolean nullable) {
    if (columnDefaultValue == null) {
      return nullable ? Literals.NULL : DEFAULT_VALUE_NOT_SET;
    }

    if (columnDefaultValue.equalsIgnoreCase(NULL)) {
      return Literals.NULL;
    }

    if (isExpression) {
      if (StringUtils.startsWithIgnoreCase(columnDefaultValue, CURRENT_TIMESTAMP)) {
        return DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
      }
      // The parsing of MySQL expressions is complex, so we are not currently undertaking the
      // parsing.
      return UnparsedExpression.of(columnDefaultValue);
    }

    switch (type.getTypeName().toLowerCase()) {
      case MysqlTypeConverter.BIT:
        if (type.getColumnSize() == null || type.getColumnSize() == 1) {
          if ("b'1'".equalsIgnoreCase(columnDefaultValue) || "1".equals(columnDefaultValue)) {
            return Literals.booleanLiteral(true);
          }
          if ("b'0'".equalsIgnoreCase(columnDefaultValue) || "0".equals(columnDefaultValue)) {
            return Literals.booleanLiteral(false);
          }
        }
        return UnparsedExpression.of(columnDefaultValue);
      case MysqlTypeConverter.TINYINT:
        return Literals.byteLiteral(Byte.valueOf(columnDefaultValue));
      case MysqlTypeConverter.TINYINT_UNSIGNED:
        return Literals.unsignedByteLiteral(Short.valueOf(columnDefaultValue));
      case MysqlTypeConverter.SMALLINT:
        return Literals.shortLiteral(Short.valueOf(columnDefaultValue));
      case MysqlTypeConverter.SMALLINT_UNSIGNED:
        return Literals.unsignedShortLiteral(Integer.valueOf(columnDefaultValue));
      case MysqlTypeConverter.INT:
        return Literals.integerLiteral(Integer.valueOf(columnDefaultValue));
      case MysqlTypeConverter.INT_UNSIGNED:
        return Literals.unsignedIntegerLiteral(Long.valueOf(columnDefaultValue));
      case MysqlTypeConverter.BIGINT:
        return Literals.longLiteral(Long.valueOf(columnDefaultValue));
      case MysqlTypeConverter.BIGINT_UNSIGNED:
        return Literals.unsignedLongLiteral(Decimal.of(columnDefaultValue));
      case MysqlTypeConverter.FLOAT:
        return Literals.floatLiteral(Float.valueOf(columnDefaultValue));
      case MysqlTypeConverter.DOUBLE:
        return Literals.doubleLiteral(Double.valueOf(columnDefaultValue));
      case MysqlTypeConverter.DECIMAL_UNSIGNED:
      case MysqlTypeConverter.DECIMAL:
        return Literals.decimalLiteral(
            Decimal.of(columnDefaultValue, type.getColumnSize(), type.getScale()));
      case JdbcTypeConverter.DATE:
        try {
          return Literals.dateLiteral(LocalDate.parse(columnDefaultValue, DATE_FORMATTER));
        } catch (DateTimeParseException e) {
          return UnparsedExpression.of(quoteStringLiteral(columnDefaultValue));
        }
      case JdbcTypeConverter.TIME:
        try {
          return Literals.timeLiteral(LocalTime.parse(columnDefaultValue, TIME_FORMATTER));
        } catch (DateTimeParseException e) {
          return UnparsedExpression.of(quoteStringLiteral(columnDefaultValue));
        }
      case JdbcTypeConverter.TIMESTAMP:
      case MysqlTypeConverter.DATETIME:
        if (StringUtils.startsWithIgnoreCase(columnDefaultValue, CURRENT_TIMESTAMP)) {
          return DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
        }
        try {
          return Literals.timestampLiteral(
              LocalDateTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
        } catch (DateTimeParseException e) {
          return UnparsedExpression.of(quoteStringLiteral(columnDefaultValue));
        }
      case JdbcTypeConverter.VARCHAR:
        return Literals.of(columnDefaultValue, Types.VarCharType.of(type.getColumnSize()));
      case MysqlTypeConverter.CHAR:
        return Literals.of(columnDefaultValue, Types.FixedCharType.of(type.getColumnSize()));
      case JdbcTypeConverter.TEXT:
        return Literals.stringLiteral(columnDefaultValue);
      default:
        return UnparsedExpression.of(columnDefaultValue);
    }
  }
}
