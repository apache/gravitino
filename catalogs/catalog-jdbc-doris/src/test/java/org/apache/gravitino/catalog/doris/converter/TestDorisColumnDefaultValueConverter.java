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

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link DorisColumnDefaultValueConverter}. */
public class TestDorisColumnDefaultValueConverter {

  private static final DorisColumnDefaultValueConverter CONVERTER =
      new DorisColumnDefaultValueConverter();

  @Test
  public void testFromGravitinoForColumnDefinitionPreservesSupportedDefaults() {
    Assertions.assertNull(
        CONVERTER.fromGravitinoForColumnDefinition(DEFAULT_VALUE_NOT_SET, false, false));
    Assertions.assertEquals(
        "NULL", CONVERTER.fromGravitinoForColumnDefinition(Literals.NULL, false, false));
    Assertions.assertEquals(
        "7", CONVERTER.fromGravitinoForColumnDefinition(Literals.integerLiteral(7), false, false));
    Assertions.assertEquals(
        "CURRENT_TIMESTAMP",
        CONVERTER.fromGravitinoForColumnDefinition(
            DEFAULT_VALUE_OF_CURRENT_TIMESTAMP, false, false));
    Assertions.assertEquals(
        "CURRENT_DATE",
        CONVERTER.fromGravitinoForColumnDefinition(
            UnparsedExpression.of("CURRENT_DATE"), false, false));
  }

  @Test
  public void testCreateTableDefinitionRejectsUnparsedDefaults() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            CONVERTER.fromGravitinoForCreateTableDefinition(UnparsedExpression.of("CURRENT_DATE")));
  }

  @Test
  public void testFromGravitinoForColumnDefinitionEscapesStringDefaultsByDorisVersion() {
    Expression defaultValue = Literals.stringLiteral("owner's\\value");

    Assertions.assertEquals(
        "\"owner's\\\\value\"",
        CONVERTER.fromGravitinoForColumnDefinition(defaultValue, false, false));
    Assertions.assertEquals(
        "\"owner's\\\\\\\\value\"",
        CONVERTER.fromGravitinoForColumnDefinition(defaultValue, true, false));
  }

  @Test
  public void testFromGravitinoForColumnDefinitionEscapesQuoteDelimiter() {
    Expression defaultValue = Literals.stringLiteral("owner's \"value\"\\path");

    Assertions.assertEquals(
        "\"" + "owner's \\\"value\\\"\\\\path" + "\"",
        CONVERTER.fromGravitinoForColumnDefinition(defaultValue, false, false));
    Assertions.assertEquals(
        "\"" + "owner's \\\"value\\\"\\\\\\\\path" + "\"",
        CONVERTER.fromGravitinoForColumnDefinition(defaultValue, true, false));
    Assertions.assertEquals(
        "\""
            + "owner's "
            + "\\".repeat(3)
            + "\""
            + "value"
            + "\\".repeat(3)
            + "\""
            + "\\".repeat(2)
            + "path\"",
        CONVERTER.fromGravitinoForColumnDefinition(defaultValue, false, true));
    Assertions.assertEquals(
        "\""
            + "owner's "
            + "\\".repeat(3)
            + "\""
            + "value"
            + "\\".repeat(3)
            + "\""
            + "\\".repeat(4)
            + "path\"",
        CONVERTER.fromGravitinoForColumnDefinition(defaultValue, true, true));
  }

  @Test
  public void testFromGravitinoForColumnDefinitionEscapesAdjacentBackslashAndQuote() {
    Expression defaultValue = Literals.stringLiteral("prefix" + "\\" + "\"" + "suffix");

    Assertions.assertEquals(
        "\"prefix" + "\\".repeat(7) + "\"suffix\"",
        CONVERTER.fromGravitinoForColumnDefinition(defaultValue, true, true));
  }

  @Test
  public void testToGravitinoUnescapesStringDefaults() {
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("varchar");
    typeBean.setColumnSize(32);

    Assertions.assertEquals(
        Literals.of("owner's\\value", Types.VarCharType.of(32)),
        CONVERTER.toGravitino(typeBean, "owner''s\\\\value", false, false));

    Assertions.assertEquals(
        Literals.stringLiteral("owner's\\value"),
        CONVERTER.toGravitino(
            new JdbcTypeConverter.JdbcTypeBean("string"), "owner''s\\\\value", false, false));
  }
}
