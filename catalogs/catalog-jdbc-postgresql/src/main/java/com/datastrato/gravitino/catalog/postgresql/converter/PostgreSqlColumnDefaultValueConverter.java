/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql.converter;

import static com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter.VARCHAR;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.BOOL;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.BPCHAR;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.DATE;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.FLOAT_4;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.FLOAT_8;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.INT_2;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.INT_4;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.INT_8;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.NUMERIC;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.TEXT;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.TIMESTAMP_TZ;
import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.UnparsedExpression;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.types.Decimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
      if (e instanceof IllegalArgumentException
          && e.getMessage().contains("Unknown data type for literal")) {
        throw e;
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
      case BOOL:
        return Literals.booleanLiteral(Boolean.valueOf(columnDefaultValue));
      case INT_2:
        return Literals.shortLiteral(Short.valueOf(columnDefaultValue));
      case INT_4:
        return Literals.integerLiteral(Integer.valueOf(columnDefaultValue));
      case INT_8:
        return Literals.longLiteral(Long.valueOf(columnDefaultValue));
      case FLOAT_4:
        return Literals.floatLiteral(Float.valueOf(columnDefaultValue));
      case FLOAT_8:
        return Literals.doubleLiteral(Double.valueOf(columnDefaultValue));
      case NUMERIC:
        return Literals.decimalLiteral(
            Decimal.of(
                columnDefaultValue,
                Integer.parseInt(type.getColumnSize()),
                Integer.parseInt(type.getScale())));
      case DATE:
        return Literals.dateLiteral(LocalDate.parse(columnDefaultValue, DATE_TIME_FORMATTER));
      case TIMESTAMP_TZ:
        return Literals.timeLiteral(LocalTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
      case VARCHAR:
        return Literals.varcharLiteral(Integer.parseInt(type.getColumnSize()), columnDefaultValue);
      case BPCHAR:
      case TEXT:
        return Literals.stringLiteral(columnDefaultValue);
      default:
        throw new IllegalArgumentException("Unknown data type for literal: " + type);
    }
  }
}
