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
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestMysqlColumnDefaultValueConverter {
  private final MysqlColumnDefaultValueConverter converter = new MysqlColumnDefaultValueConverter();

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
