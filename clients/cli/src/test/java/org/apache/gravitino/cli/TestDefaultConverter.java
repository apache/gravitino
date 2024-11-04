package org.apache.gravitino.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.junit.jupiter.api.Test;

class TestDefaultConverter {

  @Test
  void testConvertNullDefaultValue() {
    String defaultValue = null;
    String dataType = "INTEGER";
    Expression result = DefaultConverter.convert(defaultValue, dataType);

    assertEquals(
        Column.DEFAULT_VALUE_NOT_SET,
        result,
        "Expected DEFAULT_VALUE_NOT_SET for null defaultValue.");
  }

  @Test
  void testConvertEmptyDefaultValue() {
    String defaultValue = "";
    String dataType = "INTEGER";
    Expression result = DefaultConverter.convert(defaultValue, dataType);

    assertEquals(
        Column.DEFAULT_VALUE_NOT_SET,
        result,
        "Expected DEFAULT_VALUE_NOT_SET for empty defaultValue.");
  }

  @Test
  void testConvertCurrentTimestamp() {
    String defaultValue = "current_timestamp";
    String dataType = "TIMESTAMP";
    Expression result = DefaultConverter.convert(defaultValue, dataType);

    assertEquals(
        Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP,
        result,
        "Expected DEFAULT_VALUE_OF_CURRENT_TIMESTAMP for 'current_timestamp' defaultValue.");
  }

  @Test
  void testConvertLiteralValue() {
    String defaultValue = "42";
    String dataType = "INTEGER";
    Expression result = DefaultConverter.convert(defaultValue, dataType);
    Expression expected = Literals.of(defaultValue, ParseType.toType(dataType));

    assertEquals(expected, result, "Expected literal expression for non-special default value.");
  }
}
