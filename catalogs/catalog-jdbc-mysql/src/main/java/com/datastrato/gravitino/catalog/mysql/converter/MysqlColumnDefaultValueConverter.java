/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.converter;

import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.BIGINT;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.CHAR;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.DATE;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.DATETIME;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.DECIMAL;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.DOUBLE;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.FLOAT;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.INT;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.SMALLINT;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.TEXT;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.TIME;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.TIMESTAMP;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.TINYINT;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.VARCHAR;
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

public class MysqlColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {

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
      if (columnDefaultValue.equals(CURRENT_TIMESTAMP)) {
        return DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
      }
      // The parsing of MySQL expressions is complex, so we are not currently undertaking the
      // parsing.
      return UnparsedExpression.of(columnDefaultValue);
    }

    switch (type.getTypeName().toLowerCase()) {
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
                Integer.parseInt(type.getColumnSize()),
                Integer.parseInt(type.getScale())));
      case DATE:
        return Literals.dateLiteral(LocalDate.parse(columnDefaultValue, DATE_FORMATTER));
      case TIME:
        return Literals.timeLiteral(LocalTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
      case TIMESTAMP:
      case DATETIME:
        return CURRENT_TIMESTAMP.equals(columnDefaultValue)
            ? DEFAULT_VALUE_OF_CURRENT_TIMESTAMP
            : Literals.timestampLiteral(
                LocalDateTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
      case VARCHAR:
        return Literals.of(
            columnDefaultValue, Types.VarCharType.of(Integer.parseInt(type.getColumnSize())));
      case CHAR:
        return Literals.of(
            columnDefaultValue, Types.FixedCharType.of(Integer.parseInt(type.getColumnSize())));
      case TEXT:
        return Literals.stringLiteral(columnDefaultValue);
      default:
        throw new IllegalArgumentException("Unknown data type for literal: " + type);
    }
  }
}
