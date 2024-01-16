/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql.converter;

import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.BIGINT;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.BOOLEAN;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.CHAR;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.CHARACTER;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.CHARACTER_VARYING;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.DATE;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.DOUBLE_PRECISION;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.INTEGER;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.NUMERIC;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.REAL;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.SMALLINT;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.TEXT;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.TIMESTAMP_WITHOUT_TIME_ZONE;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.TIME_WITHOUT_TIME_ZONE;
import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.UnparsedExpression;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.types.Decimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.regex.Matcher;
import net.sf.jsqlparser.statement.create.table.ColDataType;

public class PostgreSqlColumnDefaultValueConverter
    extends JdbcColumnDefaultValueConverter<ColDataType, String> {
  public PostgreSqlColumnDefaultValueConverter(PostgreSqlTypeConverter postgreSqlTypeConverter) {
    super(postgreSqlTypeConverter);
  }

  @Override
  public Expression toGravitino(ColDataType type, String columnDefaultValue) {
    if (columnDefaultValue == null) {
      return DEFAULT_VALUE_NOT_SET;
    }

    Matcher expression = EXPRESSION.matcher(columnDefaultValue);
    if (expression.matches()) {
      return UnparsedExpression.of(expression.group(1));
    }

    if (columnDefaultValue.equals(CURRENT_TIMESTAMP)) {
      return DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
    }

    Matcher literal = LITERAL.matcher(columnDefaultValue);
    if (literal.matches()) {
      String literalValue = literal.group(1);
      List<String> arguments = type.getArgumentsStringList();
      switch (type.getDataType().toLowerCase()) {
        case BOOLEAN:
          return Literals.booleanLiteral(Boolean.valueOf(literalValue));
        case SMALLINT:
          return Literals.shortLiteral(Short.valueOf(literalValue));
        case INTEGER:
          return Literals.integerLiteral(Integer.valueOf(literalValue));
        case BIGINT:
          return Literals.longLiteral(Long.valueOf(literalValue));
        case REAL:
          return Literals.floatLiteral(Float.valueOf(literalValue));
        case DOUBLE_PRECISION:
          return Literals.doubleLiteral(Double.valueOf(literalValue));
        case NUMERIC:
          return Literals.decimalLiteral(
              Decimal.of(
                  literalValue,
                  Integer.parseInt(arguments.get(0)),
                  Integer.parseInt(arguments.get(1))));
        case DATE:
          return Literals.dateLiteral(LocalDate.parse(literalValue, DATE_TIME_FORMATTER));
        case TIME_WITHOUT_TIME_ZONE:
          return Literals.timeLiteral(LocalTime.parse(literalValue, DATE_TIME_FORMATTER));
        case TIMESTAMP_WITHOUT_TIME_ZONE:
          return Literals.timestampLiteral(LocalDateTime.parse(literalValue, DATE_TIME_FORMATTER));
        case CHARACTER_VARYING:
        case CHAR:
        case CHARACTER:
        case TEXT:
          return Literals.of(literalValue, jdbcTypeConverter.toGravitinoType(type));
        default:
          throw new IllegalArgumentException("Unknown data type for literal: " + type);
      }
    }

    return UnparsedExpression.of(columnDefaultValue);
  }
}
