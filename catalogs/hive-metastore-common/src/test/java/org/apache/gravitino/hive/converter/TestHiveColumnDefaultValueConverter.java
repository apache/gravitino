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
package org.apache.gravitino.hive.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDate;
import java.time.LocalDateTime;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

public class TestHiveColumnDefaultValueConverter {

  @Test
  public void testFromGravitino() {
    assertNull(HiveColumnDefaultValueConverter.fromGravitino(null));
    assertNull(HiveColumnDefaultValueConverter.fromGravitino(Column.DEFAULT_VALUE_NOT_SET));
    assertEquals("NULL", HiveColumnDefaultValueConverter.fromGravitino(Literals.NULL));
    assertEquals(
        "NULL",
        HiveColumnDefaultValueConverter.fromGravitino(Literals.of("NULL", Types.NullType.get())));
    assertEquals("42", HiveColumnDefaultValueConverter.fromGravitino(Literals.integerLiteral(42)));
    assertEquals("1.5", HiveColumnDefaultValueConverter.fromGravitino(Literals.doubleLiteral(1.5)));
    assertEquals(
        "true", HiveColumnDefaultValueConverter.fromGravitino(Literals.booleanLiteral(true)));
    assertEquals(
        "'abc'", HiveColumnDefaultValueConverter.fromGravitino(Literals.stringLiteral("abc")));
    assertEquals(
        "'it\\'s'", HiveColumnDefaultValueConverter.fromGravitino(Literals.stringLiteral("it's")));
    assertEquals(
        "'2024-01-02'",
        HiveColumnDefaultValueConverter.fromGravitino(
            Literals.dateLiteral(LocalDate.of(2024, 1, 2))));
    assertEquals(
        "'2024-01-02 03:04:05'",
        HiveColumnDefaultValueConverter.fromGravitino(
            Literals.timestampLiteral(LocalDateTime.of(2024, 1, 2, 3, 4, 5))));
    assertEquals(
        "CURRENT_TIMESTAMP()",
        HiveColumnDefaultValueConverter.fromGravitino(Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            HiveColumnDefaultValueConverter.fromGravitino(
                FunctionExpression.of("substr", Literals.stringLiteral("abc"))));
    // Unparsed expressions are written back verbatim so a loaded table can be altered as-is
    assertEquals(
        "CAST('x' AS STRING)",
        HiveColumnDefaultValueConverter.fromGravitino(
            UnparsedExpression.of("CAST('x' AS STRING)")));
    // Literal types with no defined Hive SQL rendering must not be silently serialized via
    // Object#toString()
    assertThrows(
        IllegalArgumentException.class,
        () ->
            HiveColumnDefaultValueConverter.fromGravitino(
                Literals.of(new byte[] {1, 2, 3}, Types.BinaryType.get())));
  }

  @Test
  public void testToGravitino() {
    assertEquals(
        Column.DEFAULT_VALUE_NOT_SET,
        HiveColumnDefaultValueConverter.toGravitino(Types.IntegerType.get(), null));
    assertEquals(
        Literals.NULL,
        HiveColumnDefaultValueConverter.toGravitino(Types.IntegerType.get(), "null"));
    assertEquals(
        Literals.integerLiteral(42),
        HiveColumnDefaultValueConverter.toGravitino(Types.IntegerType.get(), "42"));
    assertEquals(
        Literals.longLiteral(42L),
        HiveColumnDefaultValueConverter.toGravitino(Types.LongType.get(), "42"));
    assertEquals(
        Literals.booleanLiteral(true),
        HiveColumnDefaultValueConverter.toGravitino(Types.BooleanType.get(), "true"));
    assertEquals(
        Literals.booleanLiteral(false),
        HiveColumnDefaultValueConverter.toGravitino(Types.BooleanType.get(), " FALSE "));
    assertEquals(
        Literals.integerLiteral(-7),
        HiveColumnDefaultValueConverter.toGravitino(Types.IntegerType.get(), "-7"));
    assertEquals(
        Literals.timestampLiteral(LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123_000_000)),
        HiveColumnDefaultValueConverter.toGravitino(
            Types.TimestampType.withoutTimeZone(), "'2024-01-02 03:04:05.123'"));
    // A quoted NULL is the string "NULL", not the NULL literal
    assertEquals(
        Literals.stringLiteral("NULL"),
        HiveColumnDefaultValueConverter.toGravitino(Types.StringType.get(), "'NULL'"));
    assertEquals(
        Literals.decimalLiteral(Decimal.of("1.50", 10, 2)),
        HiveColumnDefaultValueConverter.toGravitino(Types.DecimalType.of(10, 2), "1.5"));
    assertEquals(
        Literals.stringLiteral("it's"),
        HiveColumnDefaultValueConverter.toGravitino(Types.StringType.get(), "'it\\'s'"));
    assertEquals(
        Literals.stringLiteral("it's"),
        HiveColumnDefaultValueConverter.toGravitino(Types.StringType.get(), "'it''s'"));
    assertEquals(
        Literals.varcharLiteral(10, "abc"),
        HiveColumnDefaultValueConverter.toGravitino(Types.VarCharType.of(10), "'abc'"));
    assertEquals(
        Literals.dateLiteral(LocalDate.of(2024, 1, 2)),
        HiveColumnDefaultValueConverter.toGravitino(Types.DateType.get(), "'2024-01-02'"));
    assertEquals(
        Literals.timestampLiteral(LocalDateTime.of(2024, 1, 2, 3, 4, 5)),
        HiveColumnDefaultValueConverter.toGravitino(
            Types.TimestampType.withoutTimeZone(), "'2024-01-02 03:04:05'"));
    assertEquals(
        Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP,
        HiveColumnDefaultValueConverter.toGravitino(
            Types.TimestampType.withoutTimeZone(), "CURRENT_TIMESTAMP()"));
    // Values that cannot be parsed for the column type are kept verbatim
    Expression unparsed =
        HiveColumnDefaultValueConverter.toGravitino(Types.IntegerType.get(), "abc");
    assertEquals(UnparsedExpression.of("abc"), unparsed);
    assertEquals(
        UnparsedExpression.of("'x'"),
        HiveColumnDefaultValueConverter.toGravitino(Types.BinaryType.get(), "'x'"));
    // Unquoted text on a string column and non true/false text on a boolean column are
    // expressions, not literals
    assertEquals(
        UnparsedExpression.of("CAST('x' AS STRING)"),
        HiveColumnDefaultValueConverter.toGravitino(Types.StringType.get(), "CAST('x' AS STRING)"));
    assertEquals(
        UnparsedExpression.of("CURRENT_USER"),
        HiveColumnDefaultValueConverter.toGravitino(Types.StringType.get(), "CURRENT_USER"));
    assertEquals(
        UnparsedExpression.of("CAST(1 AS BOOLEAN)"),
        HiveColumnDefaultValueConverter.toGravitino(Types.BooleanType.get(), "CAST(1 AS BOOLEAN)"));
  }

  @Test
  public void testRoundTrip() {
    assertRoundTrip(Types.StringType.get(), Literals.NULL);
    assertRoundTrip(Types.IntegerType.get(), Literals.integerLiteral(7));
    assertRoundTrip(Types.StringType.get(), Literals.stringLiteral("a'b\\c"));
    assertRoundTrip(Types.DateType.get(), Literals.dateLiteral(LocalDate.of(2024, 1, 2)));
    assertRoundTrip(
        Types.TimestampType.withoutTimeZone(), Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP);
    assertRoundTrip(Types.StringType.get(), UnparsedExpression.of("CAST('x' AS STRING)"));
  }

  private void assertRoundTrip(Type type, Expression value) {
    String hive = HiveColumnDefaultValueConverter.fromGravitino(value);
    assertEquals(value, HiveColumnDefaultValueConverter.toGravitino(type, hive));
  }
}
