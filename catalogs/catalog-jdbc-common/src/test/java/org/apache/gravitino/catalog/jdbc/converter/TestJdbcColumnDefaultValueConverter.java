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

import com.google.common.io.BaseEncoding;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
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

  @ParameterizedTest
  @ValueSource(strings = {"Z", "+08:00", "-07:00"})
  void testTimestampDefaultsUseNumericOffsets(String offset) {
    String value = "2026-11-04T12:34:56.123456" + offset;
    String expected =
        "'2026-11-04 12:34:56.123456" + (offset.equals("Z") ? "+00:00" : offset) + "'";
    Assertions.assertEquals(
        expected,
        converter.fromGravitino(
            Literals.of(OffsetDateTime.parse(value), Types.TimestampType.withTimeZone())));
    Assertions.assertEquals(
        expected, converter.fromGravitino(Literals.of(value, Types.TimestampType.withTimeZone())));
    Assertions.assertEquals(
        "'2026-11-04 12:34:56.123456'",
        converter.fromGravitino(Literals.timestampLiteral("2026-11-04T12:34:56.123456")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"12:34:00", "00:00:00"})
  void testWholeMinuteTimestampDefaultsIncludeSeconds(String time) {
    String text = "2026-11-04T" + time;
    for (String offset : new String[] {"Z", "+08:00", "-07:00"}) {
      String expected = "'2026-11-04 " + time + (offset.equals("Z") ? "+00:00" : offset) + "'";
      OffsetDateTime value = OffsetDateTime.parse(text + offset);
      Assertions.assertEquals(
          expected,
          converter.fromGravitino(Literals.of(value, Types.TimestampType.withTimeZone())));
      Assertions.assertEquals(
          expected,
          converter.fromGravitino(
              Literals.of(value.toString(), Types.TimestampType.withTimeZone())));
    }
    Assertions.assertEquals(
        "'2026-11-04 " + time + "'",
        converter.fromGravitino(Literals.timestampLiteral(LocalDateTime.parse(text))));
    Assertions.assertEquals(
        "'2026-11-04 " + time + "'",
        converter.fromGravitino(
            Literals.of(
                LocalDateTime.parse(text).toString(), Types.TimestampType.withoutTimeZone())));
  }

  @ParameterizedTest
  @ValueSource(strings = {"4142", "0080ff", ""})
  void testBinaryDefaultsPreserveBytes(String hex) {
    Assertions.assertEquals(
        "X'" + hex + "'",
        converter.fromGravitino(
            Literals.of(BaseEncoding.base16().lowerCase().decode(hex), Types.BinaryType.get())));
    Assertions.assertEquals(
        "X'" + hex + "'", converter.fromGravitino(Literals.of("0x" + hex, Types.BinaryType.get())));
    Assertions.assertEquals(
        "'legacy'", converter.fromGravitino(Literals.of("legacy", Types.BinaryType.get())));
    Assertions.assertEquals("'0x4142'", converter.fromGravitino(Literals.stringLiteral("0x4142")));
    Assertions.assertEquals(
        "X'00ff'", converter.fromGravitino(Literals.of("0x00FF", Types.BinaryType.get())));
  }

  @ParameterizedTest
  @ValueSource(strings = {"0x0", "0xzz", "0x41'); DROP TABLE t; --"})
  void testMalformedBinaryDefaultsAreRejected(String value) {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> converter.fromGravitino(Literals.of(value, Types.BinaryType.get())));
    Assertions.assertEquals(
        "Invalid binary column default value: use 0x followed by hexadecimal byte pairs",
        exception.getMessage());
    Assertions.assertNotNull(exception.getCause());
  }

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
