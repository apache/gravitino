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
package org.apache.gravitino.catalog.doris.converter;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for converting Doris column default values to SQL and Gravitino expressions. */
public class TestDorisColumnDefaultValueConverter {

  private static final DorisColumnDefaultValueConverter CONVERTER =
      new DorisColumnDefaultValueConverter();

  @Test
  public void testEscapeStringLikeDefaultValues() {
    Assertions.assertEquals(
        "\"owner's\\\\value\"",
        CONVERTER.fromGravitinoForAddColumn(
            Literals.of("owner's\\value", Types.VarCharType.of(255)), false));
    Assertions.assertEquals(
        "\"owner's\\\\value\"",
        CONVERTER.fromGravitinoForAddColumn(
            Literals.of("owner's\\value", Types.FixedCharType.of(32)), false));
    Assertions.assertEquals(
        "\"owner's\\\\value\"",
        CONVERTER.fromGravitinoForAddColumn(Literals.stringLiteral("owner's\\value"), false));
    Assertions.assertEquals(
        "\"owner's \\\"value\\\"\\\\path\"",
        CONVERTER.fromGravitinoForAddColumn(
            Literals.of("owner's \"value\"\\path", Types.VarCharType.of(255)), false));
    Assertions.assertEquals(
        "\"\"",
        CONVERTER.fromGravitinoForAddColumn(Literals.of("", Types.VarCharType.of(255)), false));
    Assertions.assertEquals(
        "\"   \"",
        CONVERTER.fromGravitinoForAddColumn(Literals.of("   ", Types.VarCharType.of(255)), false));
  }

  @Test
  public void testNumericDefaultUsesLegacyCompatibleQuotedSyntax() {
    Assertions.assertEquals(
        "\"7\"", CONVERTER.fromGravitinoForAddColumn(Literals.integerLiteral(7), false));
  }

  @Test
  public void testDelegateNullAndExpressionDefaultValues() {
    Assertions.assertEquals("NULL", CONVERTER.fromGravitinoForAddColumn(Literals.NULL, false));
    Assertions.assertEquals(
        "CURRENT_TIMESTAMP",
        CONVERTER.fromGravitinoForAddColumn(DEFAULT_VALUE_OF_CURRENT_TIMESTAMP, false));
  }

  @Test
  public void testDoubleEscapeSequencesForDoris3Alter() {
    Literal<?> defaultValue = Literals.of("owner's \"value\"\\path", Types.VarCharType.of(255));
    Assertions.assertEquals(
        "\"owner's \\\"value\\\"\\\\\\\\path\"",
        CONVERTER.fromGravitinoForAddColumn(defaultValue, true));
  }

  @Test
  public void testUnescapeStringLikeDefaultValues() {
    JdbcTypeConverter.JdbcTypeBean varcharType =
        new JdbcTypeConverter.JdbcTypeBean(JdbcTypeConverter.VARCHAR);
    varcharType.setColumnSize(255);
    JdbcTypeConverter.JdbcTypeBean charType =
        new JdbcTypeConverter.JdbcTypeBean(DorisTypeConverter.CHAR);
    charType.setColumnSize(32);

    Assertions.assertEquals(
        Literals.of("owner's\\value", Types.VarCharType.of(255)),
        CONVERTER.toGravitino(varcharType, "owner's\\\\value", false, false));
    Assertions.assertEquals(
        Literals.of("owner's\\value", Types.FixedCharType.of(32)),
        CONVERTER.toGravitino(charType, "owner's\\\\value", false, false));
    Assertions.assertEquals(
        Literals.stringLiteral("owner's\\value"),
        CONVERTER.toGravitino(
            new JdbcTypeConverter.JdbcTypeBean(JdbcTypeConverter.TEXT),
            "owner's\\\\value",
            false,
            false));
    Assertions.assertEquals(
        Literals.of("owner's \"value\"\\path", Types.VarCharType.of(255)),
        CONVERTER.toGravitino(varcharType, "owner's \\\"value\\\"\\\\path", false, false));
    Assertions.assertEquals(
        Literals.of("owner's \"value\"\\path", Types.VarCharType.of(255)),
        CONVERTER.toGravitino(varcharType, "owner's \"value\"\\path", false, false));
  }
}
