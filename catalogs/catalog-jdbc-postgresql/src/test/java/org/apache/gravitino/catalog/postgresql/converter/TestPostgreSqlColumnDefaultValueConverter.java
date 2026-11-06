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
package org.apache.gravitino.catalog.postgresql.converter;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPostgreSqlColumnDefaultValueConverter {

  private final PostgreSqlColumnDefaultValueConverter converter =
      new PostgreSqlColumnDefaultValueConverter();

  @Test
  void testNullDefaultsAllowNativeCastsWithoutSwallowingExpressions() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("uuid");
    for (String sql :
        new String[] {"NULL", "NULL::uuid", "NULL::integer[]", "NULL::\"schema\".\"domain\""}) {
      Assertions.assertEquals(Literals.NULL, converter.toGravitino(type, sql, false, true));
    }
    String expression = "NULL::uuid IS NULL";
    Assertions.assertEquals(
        UnparsedExpression.of(expression), converter.toGravitino(type, expression, false, true));
  }

  @Test
  void testBackslashNormalizationDoesNotRewriteExpressions() {
    String literal = "'a\\\\b'::text";
    Assertions.assertEquals(
        "E" + literal, PostgreSqlColumnDefaultValueConverter.withExplicitBackslashEscapes(literal));
    for (String expression :
        new String[] {
          null,
          "'plain'::text",
          "E'a\\\\b'::text",
          "upper('a\\\\b'::text)",
          "'a\\\\b'::text || 'tail'::text"
        }) {
      Assertions.assertEquals(
          expression,
          PostgreSqlColumnDefaultValueConverter.withExplicitBackslashEscapes(expression));
    }
  }

  @Test
  void testExpressionsAreNotMistakenForLiterals() {
    JdbcTypeConverter.JdbcTypeBean text = new JdbcTypeConverter.JdbcTypeBean("text");
    for (String sql :
        new String[] {
          "upper('x'::text)", "concat('a'::text, 'b'::text)", "'a'::text || 'b'::text",
          "md5(random()::text)", "('x'::text)::character varying(1)", "NULL::text || 'x'::text"
        }) {
      Assertions.assertEquals(
          UnparsedExpression.of(sql), converter.toGravitino(text, sql, false, true), sql);
    }
    String condition = "(random() > 0.5)";
    Assertions.assertEquals(
        UnparsedExpression.of(condition),
        converter.toGravitino(new JdbcTypeConverter.JdbcTypeBean("bool"), condition, false, true));
    Assertions.assertEquals(
        Literals.stringLiteral("NULL::text"),
        converter.toGravitino(text, "'NULL::text'::text", false, true));
    Assertions.assertEquals(Literals.NULL, converter.toGravitino(text, "NULL::text", false, true));
    Assertions.assertEquals(
        Literals.stringLiteral("plain"), converter.toGravitino(text, "'plain'", false, true));
  }

  @Test
  void testOtherEscapeStringSyntaxIsPreservedAsExpression() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("text");
    String sql = "E'line\\n'::text";
    Assertions.assertEquals(
        UnparsedExpression.of(sql), converter.toGravitino(type, sql, false, true));
  }

  @Test
  void testNulStringDefaultIsRejectedBeforeDdl() {
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> converter.fromGravitino(Literals.stringLiteral("before\0after")));
    Assertions.assertTrue(error.getMessage().contains("default"));
    Assertions.assertTrue(error.getMessage().contains("U+0000"));
    Assertions.assertEquals(
        "E'before\\\\u0000after'",
        converter.fromGravitino(Literals.stringLiteral("before\\u0000after")));
    Assertions.assertEquals("NULL", converter.fromGravitino(Literals.NULL));
  }

  /** Verifies long quoted text defaults can be loaded without recursive regex stack growth. */
  @Test
  public void testLongQuotedTextLiteralIsUnescaped() {
    String value = "text '::quoted \\path ".repeat(2000);
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("text");
    Assertions.assertEquals(
        Literals.stringLiteral(value),
        converter.toGravitino(
            type, converter.fromGravitino(Literals.stringLiteral(value)) + "::text", false, true));
  }

  @Test
  public void testStringLiteralDoublesQuotesOnly() {
    Assertions.assertEquals(
        "E'O''Brien \\\\path'", converter.fromGravitino(Literals.stringLiteral("O'Brien \\path")));
  }

  @Test
  public void testQuotedTextLiteralIsUnescaped() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("text");
    Assertions.assertEquals(
        Literals.stringLiteral("O'Brien'::x \\path"),
        converter.toGravitino(type, "'O''Brien''::x \\path'::text", false, true));
    Assertions.assertEquals(
        Literals.stringLiteral(""), converter.toGravitino(type, "''::text", false, true));
  }

  @Test
  void testEmptyBpcharLiteralIsUnescaped() {
    JdbcTypeConverter.JdbcTypeBean type =
        new JdbcTypeConverter.JdbcTypeBean(PostgreSqlTypeConverter.BPCHAR);
    Assertions.assertEquals(
        Literals.stringLiteral(""), converter.toGravitino(type, "''::bpchar", false, true));
  }

  @Test
  void testUnboundedVarcharDefaultIsString() {
    for (Integer length : new Integer[] {null, 0, Integer.MAX_VALUE}) {
      JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("varchar");
      type.setColumnSize(length);
      Assertions.assertEquals(
          Literals.stringLiteral("value"),
          converter.toGravitino(type, "'value'::character varying", false, true));
    }
    JdbcTypeConverter.JdbcTypeBean bounded = new JdbcTypeConverter.JdbcTypeBean("varchar");
    bounded.setColumnSize(12);
    Assertions.assertEquals(
        Literals.varcharLiteral(12, "value"),
        converter.toGravitino(bounded, "'value'::character varying", false, true));
  }
}
