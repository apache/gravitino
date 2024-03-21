/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.converter;

import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcColumnDefaultValueConverter {

  private final JdbcColumnDefaultValueConverter converter = new JdbcColumnDefaultValueConverter();

  @Test
  public void testFromGravitino() {
    Expression expression = DEFAULT_VALUE_NOT_SET;
    String result = converter.fromGravitino(expression);
    Assertions.assertNull(result);

    Expression empty = null;
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> converter.fromGravitino(empty));
    Assertions.assertEquals("Not a supported column default value: null", exception.getMessage());

    expression = Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
    result = converter.fromGravitino(expression);
    Assertions.assertEquals("CURRENT_TIMESTAMP", result);

    expression = Literals.stringLiteral("test");
    result = converter.fromGravitino(expression);
    Assertions.assertEquals("'test'", result);

    expression = Literals.NULL;
    result = converter.fromGravitino(expression);
    Assertions.assertEquals("NULL", result);

    expression = Literals.integerLiteral(1234);
    result = converter.fromGravitino(expression);
    Assertions.assertEquals("1234", result);
  }
}
