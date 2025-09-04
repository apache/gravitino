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
package org.apache.gravitino.catalog.postgresql.converter;

import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.VARCHAR;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;

public class PostgreSqlColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {
  public PostgreSqlColumnDefaultValueConverter() {}

  // for example:
  // '2021-10-01 00:00:00'::timestamp without time zone
  // ''something'::character varying'
  private static final Pattern VALUE_WITH_TYPE = Pattern.compile("'(.+?)'::\\s*(.+)");

  // for example:
  // NULL::character varying
  private static final String NULL_PREFIX = "NULL::";

  @Override
  public Expression toGravitino(
      JdbcTypeConverter.JdbcTypeBean type,
      String columnDefaultValue,
      boolean isExpression,
      boolean nullable) {
    if (columnDefaultValue == null) {
      return nullable ? Literals.NULL : DEFAULT_VALUE_NOT_SET;
    }

    if (isExpression) {
      // The parsing of Postgresql expressions is complex, so we are not currently undertaking the
      // parsing.
      return UnparsedExpression.of(columnDefaultValue);
    }

    try {
      return parseLiteral(type, columnDefaultValue);
    } catch (Exception e) {
      if (columnDefaultValue.equals(CURRENT_TIMESTAMP)) {
        return DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
      }
      return UnparsedExpression.of(columnDefaultValue);
    }
  }

  private Expression parseLiteral(JdbcTypeConverter.JdbcTypeBean type, String columnDefaultValue) {
    Matcher matcher = VALUE_WITH_TYPE.matcher(columnDefaultValue);
    if (matcher.find()) {
      columnDefaultValue = matcher.group(1);
    }

    if (columnDefaultValue.startsWith(NULL_PREFIX)) {
      return Literals.NULL;
    }

    switch (type.getTypeName().toLowerCase()) {
      case PostgreSqlTypeConverter.BOOL:
        return Literals.booleanLiteral(Boolean.valueOf(columnDefaultValue));
      case PostgreSqlTypeConverter.INT_2:
        return Literals.shortLiteral(Short.valueOf(columnDefaultValue));
      case PostgreSqlTypeConverter.INT_4:
        return Literals.integerLiteral(Integer.valueOf(columnDefaultValue));
      case PostgreSqlTypeConverter.INT_8:
        return Literals.longLiteral(Long.valueOf(columnDefaultValue));
      case PostgreSqlTypeConverter.FLOAT_4:
        return Literals.floatLiteral(Float.valueOf(columnDefaultValue));
      case PostgreSqlTypeConverter.FLOAT_8:
        return Literals.doubleLiteral(Double.valueOf(columnDefaultValue));
      case PostgreSqlTypeConverter.NUMERIC:
        return Literals.decimalLiteral(
            Decimal.of(columnDefaultValue, type.getColumnSize(), type.getScale()));
      case JdbcTypeConverter.DATE:
        return Literals.dateLiteral(LocalDate.parse(columnDefaultValue, DATE_FORMATTER));
      case JdbcTypeConverter.TIME:
        return Literals.timeLiteral(LocalTime.parse(columnDefaultValue, TIME_FORMATTER));
      case JdbcTypeConverter.TIMESTAMP:
      case PostgreSqlTypeConverter.TIMESTAMP_TZ:
        return Literals.timestampLiteral(
            LocalDateTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
      case VARCHAR:
        return Literals.varcharLiteral(type.getColumnSize(), columnDefaultValue);
      case PostgreSqlTypeConverter.BPCHAR:
      case JdbcTypeConverter.TEXT:
        return Literals.stringLiteral(columnDefaultValue);
      default:
        throw new IllegalArgumentException("Unknown data type for literal: " + type);
    }
  }
}
