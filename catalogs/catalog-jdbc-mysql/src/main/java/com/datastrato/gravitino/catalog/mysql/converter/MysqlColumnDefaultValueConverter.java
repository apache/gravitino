/*
 * Copyright 2023 Datastrato Pvt Ltd.
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

public class MysqlColumnDefaultValueConverter
    extends JdbcColumnDefaultValueConverter<ColDataType, String> {

  public MysqlColumnDefaultValueConverter(MysqlTypeConverter mysqlTypeConverter) {
    super(mysqlTypeConverter);
  }

  @Override
  public Expression toGravitino(ColDataType type, String columnDefaultValue) {
    if (columnDefaultValue == null) {
      return DEFAULT_VALUE_NOT_SET;
    }

    if (columnDefaultValue.equalsIgnoreCase(NULL)) {
      return Literals.NULL;
    }

    // MySQL enclose expression default values within parentheses to distinguish them from literal
    // constant default values. see
    // https://dev.mysql.com/doc/refman/8.0/en/data-type-defaults.html#data-type-defaults-explicit
    Matcher expression = EXPRESSION.matcher(columnDefaultValue);
    if (expression.matches()) {
      // The parsing of MySQL expressions is complex, so we are not currently undertaking the
      // parsing.
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
        case TINYINT:
          return Literals.byteLiteral(Byte.valueOf(literalValue));
        case SMALLINT:
          return Literals.shortLiteral(Short.valueOf(literalValue));
        case INT:
          return Literals.integerLiteral(Integer.valueOf(literalValue));
        case BIGINT:
          return Literals.longLiteral(Long.valueOf(literalValue));
        case FLOAT:
          return Literals.floatLiteral(Float.valueOf(literalValue));
        case DOUBLE:
          return Literals.doubleLiteral(Double.valueOf(literalValue));
        case DECIMAL:
          return Literals.decimalLiteral(
              Decimal.of(
                  literalValue,
                  Integer.parseInt(arguments.get(0)),
                  Integer.parseInt(arguments.get(1))));
        case DATE:
          return Literals.dateLiteral(LocalDate.parse(literalValue, DATE_TIME_FORMATTER));
        case TIME:
          return Literals.timeLiteral(LocalTime.parse(literalValue, DATE_TIME_FORMATTER));
        case TIMESTAMP:
        case DATETIME:
          return Literals.timestampLiteral(LocalDateTime.parse(literalValue, DATE_TIME_FORMATTER));
        case VARCHAR:
        case CHAR:
        case TEXT:
          return Literals.of(literalValue, jdbcTypeConverter.toGravitinoType(type));
        default:
          throw new IllegalArgumentException("Unknown data type for literal: " + type);
      }
    }

    return UnparsedExpression.of(columnDefaultValue);
  }
}
