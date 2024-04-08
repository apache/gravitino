/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.converter;

import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.FunctionExpression;
import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.types.Type;
import java.time.format.DateTimeFormatter;

public class JdbcColumnDefaultValueConverter {

  protected static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
  protected static final String NULL = "NULL";
  protected static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  protected static final DateTimeFormatter DATE_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd");

  public String fromGravitino(Expression defaultValue) {
    if (DEFAULT_VALUE_NOT_SET.equals(defaultValue)) {
      return null;
    }

    if (defaultValue instanceof FunctionExpression) {
      FunctionExpression functionExpression = (FunctionExpression) defaultValue;
      if (functionExpression.functionName().equalsIgnoreCase(CURRENT_TIMESTAMP)) {
        // CURRENT_TIMESTAMP is a special case(key word), it should not be wrapped in parentheses
        return CURRENT_TIMESTAMP;
      } else {
        return String.format("(%s)", functionExpression);
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

  public Expression toGravitino(
      JdbcTypeConverter.JdbcTypeBean columnType,
      String columnDefaultValue,
      boolean isExpression,
      boolean nullable) {
    return DEFAULT_VALUE_NOT_SET;
  }
}
