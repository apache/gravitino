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
package org.apache.gravitino.catalog.clickhouse.converter;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestClickHouseColumnDefaultValueConverter {

  private static final ClickHouseColumnDefaultValueConverter CONVERTER =
      new ClickHouseColumnDefaultValueConverter();

  @Test
  void testStringDefaultRoundTrip() {
    String[] values = {"a'b", "a\\b", "it''s", ""};
    for (String value : values) {
      String sql = CONVERTER.fromGravitino(Literals.stringLiteral(value));
      // ClickHouse reports string defaults with backslash escapes.
      String reported = "'" + sql.substring(1, sql.length() - 1).replace("''", "\\'") + "'";
      Assertions.assertEquals(
          Literals.stringLiteral(value),
          CONVERTER.toGravitino(
              new JdbcTypeConverter.JdbcTypeBean("String"), reported, false, true),
          "Round trip failed for " + value);
    }
  }

  @Test
  void testStringDefaultUnescape() {
    JdbcTypeConverter.JdbcTypeBean stringType = new JdbcTypeConverter.JdbcTypeBean("String");
    Assertions.assertEquals(
        Literals.stringLiteral("a'b"), CONVERTER.toGravitino(stringType, "'a\\'b'", false, true));
    Assertions.assertEquals(
        Literals.stringLiteral("a'b"), CONVERTER.toGravitino(stringType, "'a''b'", false, true));
    Assertions.assertEquals(
        Literals.stringLiteral("a\\b"), CONVERTER.toGravitino(stringType, "'a\\\\b'", false, true));
    Assertions.assertEquals(
        Literals.stringLiteral("a\n\t\r\0\b\f\u0007\u000Bz"),
        CONVERTER.toGravitino(stringType, "'a\\n\\t\\r\\0\\b\\f\\a\\v\\x7a'", false, true));
    Assertions.assertEquals(
        Literals.stringLiteral("xg1"), CONVERTER.toGravitino(stringType, "'\\xg1'", false, true));
  }

  @Test
  void testUnparsedDefaultKeepsServerText() {
    JdbcTypeConverter.JdbcTypeBean intType = new JdbcTypeConverter.JdbcTypeBean("Int32");
    Assertions.assertEquals(
        UnparsedExpression.of("a\\'b"), CONVERTER.toGravitino(intType, "'a\\'b'", false, true));
  }

  @Test
  void testValidDefaultValues() {
    assertAccepted(Types.ByteType.get(), "-128");
    assertAccepted(Types.ByteType.get(), "127");
    assertAccepted(Types.ByteType.unsigned(), "255");
    assertAccepted(Types.ShortType.unsigned(), "65535");
    assertAccepted(Types.IntegerType.get(), "-2147483648");
    assertAccepted(Types.IntegerType.unsigned(), "4294967295");
    assertAccepted(Types.LongType.unsigned(), "18446744073709551615");
    assertAccepted(Types.DecimalType.of(3, 2), "9.99");
    assertAccepted(Types.DateType.get(), "1970-01-01");
    assertAccepted(Types.DateType.get(), "2149-06-06");
    assertAccepted(Types.UUIDType.get(), "61f0c404-5cb3-11e7-907b-a6006ad3dba0");
    assertAccepted(Types.DecimalType.of(3, 2), "9.994");
    assertAccepted(Types.DecimalType.of(3, 2), "-9.99");
    // Callers may wrap the value in single quotes.
    assertAccepted(Types.DateType.get(), "'2024-01-01'");
    assertAccepted(Types.UUIDType.get(), "'61f0c404-5cb3-11e7-907b-a6006ad3dba0'");
    assertAccepted(Types.IntegerType.get(), "'5'");
  }

  @Test
  void testNonLiteralDefaultValuesSkipValidation() {
    Assertions.assertDoesNotThrow(
        () -> CONVERTER.checkDefaultValue(Types.IntegerType.get(), Literals.NULL));
    Assertions.assertDoesNotThrow(
        () -> CONVERTER.checkDefaultValue(Types.DateType.get(), FunctionExpression.of("today")));
    Assertions.assertDoesNotThrow(
        () ->
            CONVERTER.checkDefaultValue(
                Types.TimestampType.withoutTimeZone(), Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP));
    Assertions.assertDoesNotThrow(
        () ->
            CONVERTER.checkDefaultValue(
                Types.UUIDType.get(), UnparsedExpression.of("generateUUIDv4()")));
  }

  @Test
  void testInvalidDefaultValues() {
    assertRejected(Types.ByteType.get(), "-129");
    assertRejected(Types.ByteType.get(), "128");
    assertRejected(Types.ByteType.unsigned(), "-1");
    assertRejected(Types.ByteType.unsigned(), "256");
    assertRejected(Types.ShortType.get(), "32768");
    assertRejected(Types.IntegerType.get(), "2147483648");
    assertRejected(Types.IntegerType.unsigned(), "4294967296");
    assertRejected(Types.LongType.get(), "9223372036854775808");
    assertRejected(Types.IntegerType.get(), "abc");
    assertRejected(Types.DecimalType.of(3, 2), "99.999");
    assertRejected(Types.DecimalType.of(3, 2), "9.995");
    assertRejected(Types.DateType.get(), "'2150-01-01'");
    assertRejected(Types.DateType.get(), "1969-12-31");
    assertRejected(Types.DateType.get(), "2150-01-01");
    assertRejected(Types.DateType.get(), "2024-13-01");
    assertRejected(Types.UUIDType.get(), "not-a-uuid");
  }

  private static void assertAccepted(Type type, String value) {
    Assertions.assertDoesNotThrow(
        () -> CONVERTER.checkDefaultValue(type, Literals.of(value, type)));
  }

  private static void assertRejected(Type type, String value) {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CONVERTER.checkDefaultValue(type, Literals.of(value, type)),
        "Expected rejection of " + value + " for " + type.simpleString());
  }
}
