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
package org.apache.gravitino.catalog.mysql.converter;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestMysqlColumnDefaultValueConverter {
  private final MysqlColumnDefaultValueConverter converter = new MysqlColumnDefaultValueConverter();

  @ParameterizedTest
  @CsvSource({
    "DATE, 0000-00-00",
    "TIMESTAMP, 0000-00-00 00:00:00",
    "DATETIME, 0000-00-00 00:00:00",
    "TIMESTAMP, 0000-00-00 00:00:00.0",
    "DATETIME, 0000-00-00 00:00:00.000",
    "TIMESTAMP, 0000-00-00 00:00:00.000000",
    "DATETIME, 0000-00-00 00:00:00.123456"
  })
  void testZeroDateDefaultsArePreserved(String typeName, String value) {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean(typeName);
    Expression result = converter.toGravitino(type, value, false, false);
    String sql = "'" + value + "'";
    Assertions.assertEquals(UnparsedExpression.of(sql), result);
    Assertions.assertEquals(result, converter.toGravitino(type, value, false, true));
  }

  @ParameterizedTest
  @ValueSource(strings = {"TIMESTAMP", "DATETIME"})
  void testValidTemporalDefaults(String typeName) {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean(typeName);
    Assertions.assertEquals(
        Literals.timestampLiteral("2024-01-02T03:04:05.123456"),
        converter.toGravitino(type, "2024-01-02 03:04:05.123456", false, false));
    Assertions.assertEquals(
        Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP,
        converter.toGravitino(type, "CURRENT_TIMESTAMP(6)", false, false));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> converter.toGravitino(type, "invalid datetime", false, false));
  }

  @Test
  void testValidDateDefault() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("DATE");
    Assertions.assertEquals(
        Literals.dateLiteral("2024-01-02"),
        converter.toGravitino(type, "2024-01-02", false, false));
  }

  @ParameterizedTest
  @ValueSource(strings = {"true", "TRUE", "1"})
  void testTrueDefaultsAreUnquoted(String value) {
    Assertions.assertEquals(
        "1", converter.fromGravitino(Literals.of(value, Types.BooleanType.get())));
  }

  @ParameterizedTest
  @ValueSource(strings = {"false", "FALSE", "0"})
  void testFalseDefaultsAreUnquoted(String value) {
    Assertions.assertEquals(
        "0", converter.fromGravitino(Literals.of(value, Types.BooleanType.get())));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "2", "-1", "truthy", "true OR 1=1"})
  void testInvalidBooleanDefaultsAreRejected(String value) {
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> converter.fromGravitino(Literals.of(value, Types.BooleanType.get())));
    Assertions.assertTrue(error.getMessage().contains("boolean"));
    Assertions.assertTrue(error.getMessage().contains("use true, false, 1 or 0"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"b'1'", "B'1'", "1"})
  void testBitOneDefaultsLoadAsTrue(String value) {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("BIT");
    type.setColumnSize(1);
    Assertions.assertEquals(
        Literals.booleanLiteral(true), converter.toGravitino(type, value, false, false));
  }

  @ParameterizedTest
  @ValueSource(strings = {"b'0'", "B'0'", "0"})
  void testBitOneDefaultsLoadAsFalse(String value) {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("bit");
    type.setColumnSize(1);
    Assertions.assertEquals(
        Literals.booleanLiteral(false), converter.toGravitino(type, value, false, false));
  }

  @Test
  void testOtherDefaultsKeepTheirMeaning() {
    Assertions.assertEquals("1", converter.fromGravitino(Literals.booleanLiteral(true)));
    Assertions.assertEquals("0", converter.fromGravitino(Literals.booleanLiteral(false)));
    Assertions.assertEquals("'true'", converter.fromGravitino(Literals.stringLiteral("true")));
    Assertions.assertEquals("NULL", converter.fromGravitino(Literals.NULL));
    Assertions.assertNull(converter.fromGravitino(Column.DEFAULT_VALUE_NOT_SET));
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("bit");
    type.setColumnSize(8);
    Assertions.assertEquals(
        UnparsedExpression.of("b'10'"), converter.toGravitino(type, "b'10'", false, false));
    type.setColumnSize(null);
    Assertions.assertEquals(
        Literals.booleanLiteral(true), converter.toGravitino(type, "b'1'", false, false));
    type.setColumnSize(1);
    Assertions.assertEquals(
        UnparsedExpression.of("b'10'"), converter.toGravitino(type, "b'10'", false, false));
    Assertions.assertEquals(
        UnparsedExpression.of("unknown()"), converter.toGravitino(type, "unknown()", true, false));
    Assertions.assertEquals(Literals.NULL, converter.toGravitino(type, null, false, true));
  }
}
