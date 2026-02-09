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
package org.apache.gravitino.catalog.hologres.converter;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link HologresColumnDefaultValueConverter}. */
public class TestHologresColumnDefaultValueConverter {

  private final HologresColumnDefaultValueConverter converter =
      new HologresColumnDefaultValueConverter();

  @Test
  public void testNullDefaultValueNullable() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("int4");
    Expression result = converter.toGravitino(type, null, false, true);
    Assertions.assertEquals(Literals.NULL, result);
  }

  @Test
  public void testNullDefaultValueNotNullable() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("int4");
    Expression result = converter.toGravitino(type, null, false, false);
    Assertions.assertEquals(DEFAULT_VALUE_NOT_SET, result);
  }

  @Test
  public void testExpressionReturnsUnparsed() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("int4");
    Expression result = converter.toGravitino(type, "nextval('seq')", true, true);
    Assertions.assertInstanceOf(UnparsedExpression.class, result);
    Assertions.assertEquals("nextval('seq')", ((UnparsedExpression) result).unparsedExpression());
  }

  @Test
  public void testBooleanLiteral() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("bool");
    Expression result = converter.toGravitino(type, "true", false, true);
    Assertions.assertEquals(Literals.booleanLiteral(true), result);
  }

  @Test
  public void testBooleanLiteralFalse() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("bool");
    Expression result = converter.toGravitino(type, "false", false, true);
    Assertions.assertEquals(Literals.booleanLiteral(false), result);
  }

  @Test
  public void testShortLiteral() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("int2");
    Expression result = converter.toGravitino(type, "42", false, true);
    Assertions.assertEquals(Literals.shortLiteral((short) 42), result);
  }

  @Test
  public void testIntegerLiteral() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("int4");
    Expression result = converter.toGravitino(type, "100", false, true);
    Assertions.assertEquals(Literals.integerLiteral(100), result);
  }

  @Test
  public void testLongLiteral() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("int8");
    Expression result = converter.toGravitino(type, "9999999999", false, true);
    Assertions.assertEquals(Literals.longLiteral(9999999999L), result);
  }

  @Test
  public void testFloatLiteral() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("float4");
    Expression result = converter.toGravitino(type, "3.14", false, true);
    Assertions.assertEquals(Literals.floatLiteral(3.14f), result);
  }

  @Test
  public void testDoubleLiteral() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("float8");
    Expression result = converter.toGravitino(type, "3.14159265", false, true);
    Assertions.assertEquals(Literals.doubleLiteral(3.14159265), result);
  }

  @Test
  public void testDecimalLiteral() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("numeric");
    type.setColumnSize(10);
    type.setScale(2);
    Expression result = converter.toGravitino(type, "'12.34'::numeric", false, true);
    Assertions.assertEquals(
        Literals.decimalLiteral(org.apache.gravitino.rel.types.Decimal.of("12.34", 10, 2)), result);
  }

  @Test
  public void testVarcharLiteral() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("varchar");
    type.setColumnSize(100);
    Expression result =
        converter.toGravitino(type, "'hello world'::character varying", false, true);
    Assertions.assertEquals(Literals.varcharLiteral(100, "hello world"), result);
  }

  @Test
  public void testTextLiteral() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("text");
    Expression result = converter.toGravitino(type, "'some text'::text", false, true);
    Assertions.assertEquals(Literals.stringLiteral("some text"), result);
  }

  @Test
  public void testBpcharLiteral() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("bpchar");
    type.setColumnSize(10);
    Expression result = converter.toGravitino(type, "'abc'::bpchar", false, true);
    Assertions.assertEquals(Literals.stringLiteral("abc"), result);
  }

  @Test
  public void testDateLiteral() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("date");
    Expression result = converter.toGravitino(type, "'2024-01-15'::date", false, true);
    Assertions.assertEquals(Literals.dateLiteral(java.time.LocalDate.of(2024, 1, 15)), result);
  }

  @Test
  public void testTimeLiteral() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("time");
    Expression result =
        converter.toGravitino(type, "'10:30:00'::time without time zone", false, true);
    Assertions.assertEquals(Literals.timeLiteral(java.time.LocalTime.of(10, 30, 0)), result);
  }

  @Test
  public void testTimestampLiteral() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("timestamp");
    Expression result =
        converter.toGravitino(
            type, "'2024-01-15 10:30:00'::timestamp without time zone", false, true);
    Assertions.assertEquals(
        Literals.timestampLiteral(java.time.LocalDateTime.of(2024, 1, 15, 10, 30, 0)), result);
  }

  @Test
  public void testCurrentTimestamp() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("timestamptz");
    // When parseLiteral fails but value equals CURRENT_TIMESTAMP
    Expression result = converter.toGravitino(type, "CURRENT_TIMESTAMP", false, true);
    Assertions.assertEquals(DEFAULT_VALUE_OF_CURRENT_TIMESTAMP, result);
  }

  @Test
  public void testNullCastReturnsNull() {
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("varchar");
    type.setColumnSize(100);
    Expression result = converter.toGravitino(type, "NULL::character varying", false, true);
    Assertions.assertEquals(Literals.NULL, result);
  }

  @Test
  public void testUnknownTypeReturnsUnparsed() {
    // When parseLiteral throws for unknown type and it's not CURRENT_TIMESTAMP
    JdbcTypeConverter.JdbcTypeBean type = new JdbcTypeConverter.JdbcTypeBean("unknown_type");
    Expression result = converter.toGravitino(type, "some_value", false, true);
    Assertions.assertInstanceOf(UnparsedExpression.class, result);
  }
}
