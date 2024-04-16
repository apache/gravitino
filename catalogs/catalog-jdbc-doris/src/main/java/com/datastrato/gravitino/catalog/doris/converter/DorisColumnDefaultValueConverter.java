/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.converter;

import static com.datastrato.gravitino.catalog.doris.converter.DorisTypeConverter.BIGINT;
import static com.datastrato.gravitino.catalog.doris.converter.DorisTypeConverter.CHAR;
import static com.datastrato.gravitino.catalog.doris.converter.DorisTypeConverter.DATETIME;
import static com.datastrato.gravitino.catalog.doris.converter.DorisTypeConverter.DECIMAL;
import static com.datastrato.gravitino.catalog.doris.converter.DorisTypeConverter.DOUBLE;
import static com.datastrato.gravitino.catalog.doris.converter.DorisTypeConverter.FLOAT;
import static com.datastrato.gravitino.catalog.doris.converter.DorisTypeConverter.INT;
import static com.datastrato.gravitino.catalog.doris.converter.DorisTypeConverter.SMALLINT;
import static com.datastrato.gravitino.catalog.doris.converter.DorisTypeConverter.TINYINT;
import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.UnparsedExpression;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.types.Decimal;
import com.datastrato.gravitino.rel.types.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class DorisColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {

  @Override
  public Expression toGravitino(
      JdbcTypeConverter.JdbcTypeBean columnType,
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
      if (columnDefaultValue.equals(CURRENT_TIMESTAMP)) {
        return DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
      }
      // The parsing of Doris expressions is complex, so we are not currently undertaking the
      // parsing.
      return UnparsedExpression.of(columnDefaultValue);
    }

    switch (columnType.getTypeName().toLowerCase()) {
      case TINYINT:
        return Literals.byteLiteral(Byte.valueOf(columnDefaultValue));
      case SMALLINT:
        return Literals.shortLiteral(Short.valueOf(columnDefaultValue));
      case INT:
        return Literals.integerLiteral(Integer.valueOf(columnDefaultValue));
      case BIGINT:
        return Literals.longLiteral(Long.valueOf(columnDefaultValue));
      case FLOAT:
        return Literals.floatLiteral(Float.valueOf(columnDefaultValue));
      case DOUBLE:
        return Literals.doubleLiteral(Double.valueOf(columnDefaultValue));
      case DECIMAL:
        return Literals.decimalLiteral(
            Decimal.of(
                columnDefaultValue,
                Integer.parseInt(columnType.getColumnSize()),
                Integer.parseInt(columnType.getScale())));
      case JdbcTypeConverter.DATE:
        return Literals.dateLiteral(LocalDate.parse(columnDefaultValue, DATE_TIME_FORMATTER));
      case JdbcTypeConverter.TIME:
        return Literals.timeLiteral(LocalTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
      case JdbcTypeConverter.TIMESTAMP:
      case DATETIME:
        return CURRENT_TIMESTAMP.equals(columnDefaultValue)
            ? DEFAULT_VALUE_OF_CURRENT_TIMESTAMP
            : Literals.timestampLiteral(
                LocalDateTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
      case JdbcTypeConverter.VARCHAR:
        return Literals.of(
            columnDefaultValue, Types.VarCharType.of(Integer.parseInt(columnType.getColumnSize())));
      case CHAR:
        return Literals.of(
            columnDefaultValue,
            Types.FixedCharType.of(Integer.parseInt(columnType.getColumnSize())));
      case JdbcTypeConverter.TEXT:
        return Literals.stringLiteral(columnDefaultValue);
      default:
        throw new IllegalArgumentException("Unknown data columnType for literal: " + columnType);
    }
  }
}
