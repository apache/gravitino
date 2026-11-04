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
package org.apache.gravitino.catalog.jdbc.converter;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestJdbcColumnDefaultValueConverter {

  private final JdbcColumnDefaultValueConverter converter = new JdbcColumnDefaultValueConverter();

  @Test
  void testFunctionArgumentsAreRenderedAsSqlLiterals() {
    Assertions.assertEquals(
        "(abs(-3))",
        converter.fromGravitino(FunctionExpression.of("abs", Literals.integerLiteral(-3))));
    Assertions.assertEquals(
        "(pg_catalog.abs(-3))",
        converter.fromGravitino(
            FunctionExpression.of("pg_catalog.abs", Literals.integerLiteral(-3))));
    Assertions.assertEquals(
        "(coalesce(NULL, 'O''Brien'))",
        converter.fromGravitino(
            FunctionExpression.of("coalesce", Literals.NULL, Literals.stringLiteral("O'Brien"))));
    Assertions.assertEquals(
        "(date_trunc('day', (now())))",
        converter.fromGravitino(
            FunctionExpression.of(
                "date_trunc", Literals.stringLiteral("day"), FunctionExpression.of("now"))));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "abs(1)); CREATE TABLE injected_marker(id int); --",
        "abs, extra int",
        "abs/*comment*/",
        "abs--comment",
        "abs()",
        "",
        "pg_catalog..abs"
      })
  void testFunctionNameMustBeAnIdentifier(String name) {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> converter.fromGravitino(FunctionExpression.of(name)));
  }

  @Test
  void testDeeplyQualifiedFunctionNameIsRejected() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> converter.fromGravitino(FunctionExpression.of("schema.".repeat(10000) + "abs")));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> converter.fromGravitino(FunctionExpression.of(null)));
  }

  @Test
  void testFunctionArgumentsMustBeRenderableValues() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.fromGravitino(
                FunctionExpression.of(
                    "abs", UnparsedExpression.of("1); CREATE TABLE injected_marker(id int); --"))));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> converter.fromGravitino(FunctionExpression.of("abs", DEFAULT_VALUE_NOT_SET)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> converter.fromGravitino(FunctionExpression.of("abs", new Expression[] {null})));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> converter.fromGravitino(FunctionExpression.of("abs", (Expression[]) null)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.fromGravitino(
                FunctionExpression.of("CURRENT_TIMESTAMP", Literals.integerLiteral(1))));
  }

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

  @Test
  public void testStringLiteralIsEscaped() {
    Assertions.assertEquals(
        "'O''Brien'", converter.fromGravitino(Literals.stringLiteral("O'Brien")));
    Assertions.assertEquals(
        "'a\\\\'' , 1); DROP TABLE t; --'",
        converter.fromGravitino(Literals.varcharLiteral(64, "a\\' , 1); DROP TABLE t; --")));
    Assertions.assertEquals(
        "'it''s'", converter.fromGravitino(Literals.of("it's", Types.FixedCharType.of(8))));
    Assertions.assertEquals(
        "'2024-01-01 00:00:00'''",
        converter.fromGravitino(
            Literals.of("2024-01-01T00:00:00'", Types.TimestampType.withoutTimeZone())));
    Assertions.assertEquals(
        "'2024-01-01'''",
        converter.fromGravitino(Literals.of("2024-01-01'", Types.DateType.get())));
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "-12", "+3", "1.50", " 42 ", "1.0E10", "-2.5e-3"})
  public void testValidNumericLiteralIsWritten(String value) {
    Assertions.assertEquals(
        value.trim(), converter.fromGravitino(Literals.of(value, Types.DecimalType.of(20, 4))));
  }

  @ParameterizedTest
  @ValueSource(strings = {"1); DROP TABLE t; --", "1 OR 1=1", "abc", "NaN", "0x10", "1,000"})
  public void testInvalidNumericLiteralIsRejected(String value) {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> converter.fromGravitino(Literals.of(value, Types.IntegerType.get())));
    Assertions.assertEquals(
        "Invalid numeric column default value: " + value, exception.getMessage());
  }

  @Test
  public void testBlankNumericLiteralKeepsQuotedEmptyString() {
    Assertions.assertEquals(
        "''", converter.fromGravitino(Literals.of("", Types.IntegerType.get())));
  }
}
