/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.converter;

import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.FunctionExpression;
import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.types.Type;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

public abstract class JdbcColumnDefaultValueConverter<T, V> {

  protected static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
  protected static final String NULL = "NULL";
  private static final String CURRENT_DATE = "curdate()";
  protected static final Pattern EXPRESSION = Pattern.compile("^\\((.*)\\)$");
  protected static final Pattern LITERAL = Pattern.compile("^'(.*?)'$");
  protected static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  protected final JdbcTypeConverter jdbcTypeConverter;

  public JdbcColumnDefaultValueConverter(JdbcTypeConverter jdbcTypeConverter) {
    this.jdbcTypeConverter = jdbcTypeConverter;
  }

  public String fromGravitino(Column column) {
    Expression defaultValue = column.defaultValue();
    if (DEFAULT_VALUE_NOT_SET.equals(defaultValue)) {
      return null;
    }

    if (defaultValue instanceof FunctionExpression) {
      FunctionExpression functionExpression = (FunctionExpression) defaultValue;
      if (functionExpression.functionName().equalsIgnoreCase(CURRENT_TIMESTAMP)) {
        return CURRENT_TIMESTAMP;
      } else {
        throw new IllegalArgumentException(
            "Not a supported function expression: " + functionExpression);
      }
    }

    if (defaultValue instanceof Literal) {
      Literal<?> literal = (Literal<?>) defaultValue;
      Type type = literal.dataType();
      if (defaultValue.equals(Literals.NULL)) {
        return NULL;
      } else if (type instanceof Type.NumericType) {
        return literal.value().toString();
      } else {
        return String.format("'%s'", literal.value());
      }
    }

    throw new IllegalArgumentException("Not a supported column default value: " + defaultValue);
  }

  public abstract Expression toGravitino(T columnType, V columnDefaultValue);
}
