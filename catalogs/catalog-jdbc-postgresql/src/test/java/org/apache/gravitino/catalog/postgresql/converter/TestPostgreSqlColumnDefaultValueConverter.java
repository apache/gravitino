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
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPostgreSqlColumnDefaultValueConverter {

  private final PostgreSqlColumnDefaultValueConverter converter =
      new PostgreSqlColumnDefaultValueConverter();

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
        "'O''Brien \\path'", converter.fromGravitino(Literals.stringLiteral("O'Brien \\path")));
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
}
