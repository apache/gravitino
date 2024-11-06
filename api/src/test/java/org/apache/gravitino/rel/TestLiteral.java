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
package org.apache.gravitino.rel;

import static org.apache.gravitino.rel.expressions.literals.Literals.booleanLiteral;
import static org.apache.gravitino.rel.expressions.literals.Literals.byteLiteral;
import static org.apache.gravitino.rel.expressions.literals.Literals.dateLiteral;
import static org.apache.gravitino.rel.expressions.literals.Literals.decimalLiteral;
import static org.apache.gravitino.rel.expressions.literals.Literals.doubleLiteral;
import static org.apache.gravitino.rel.expressions.literals.Literals.floatLiteral;
import static org.apache.gravitino.rel.expressions.literals.Literals.integerLiteral;
import static org.apache.gravitino.rel.expressions.literals.Literals.longLiteral;
import static org.apache.gravitino.rel.expressions.literals.Literals.shortLiteral;
import static org.apache.gravitino.rel.expressions.literals.Literals.stringLiteral;
import static org.apache.gravitino.rel.expressions.literals.Literals.timeLiteral;
import static org.apache.gravitino.rel.expressions.literals.Literals.timestampLiteral;
import static org.apache.gravitino.rel.expressions.literals.Literals.unsignedByteLiteral;
import static org.apache.gravitino.rel.expressions.literals.Literals.unsignedIntegerLiteral;
import static org.apache.gravitino.rel.expressions.literals.Literals.unsignedLongLiteral;
import static org.apache.gravitino.rel.expressions.literals.Literals.unsignedShortLiteral;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLiteral {

  @Test
  public void testLiterals() {
    Literal<?> literal = booleanLiteral(Boolean.valueOf("true"));
    Assertions.assertEquals(true, literal.value());
    Assertions.assertEquals(Types.BooleanType.get(), literal.dataType());

    literal = byteLiteral(Byte.valueOf("1"));
    Assertions.assertEquals((byte) 1, literal.value());
    Assertions.assertEquals(Types.ByteType.get(), literal.dataType());

    literal = unsignedByteLiteral(Short.valueOf("1"));
    Assertions.assertEquals((short) 1, literal.value());
    Assertions.assertEquals(Types.ByteType.unsigned(), literal.dataType());

    literal = shortLiteral(Short.valueOf("1"));
    Assertions.assertEquals((short) 1, literal.value());
    Assertions.assertEquals(Types.ShortType.get(), literal.dataType());

    literal = unsignedShortLiteral(Integer.valueOf("1"));
    Assertions.assertEquals(1, literal.value());
    Assertions.assertEquals(Types.ShortType.unsigned(), literal.dataType());

    literal = integerLiteral(Integer.valueOf("1"));
    Assertions.assertEquals(1, literal.value());
    Assertions.assertEquals(Types.IntegerType.get(), literal.dataType());

    literal = unsignedIntegerLiteral(Long.valueOf("1"));
    Assertions.assertEquals(1L, literal.value());
    Assertions.assertEquals(Types.IntegerType.unsigned(), literal.dataType());

    literal = longLiteral(Long.valueOf("1"));
    Assertions.assertEquals(1L, literal.value());
    Assertions.assertEquals(Types.LongType.get(), literal.dataType());

    literal = unsignedLongLiteral(Decimal.of("1"));
    Assertions.assertEquals(Decimal.of("1"), literal.value());
    Assertions.assertEquals(Types.LongType.unsigned(), literal.dataType());

    literal = floatLiteral(Float.valueOf("1.234"));
    Assertions.assertEquals(1.234f, literal.value());
    Assertions.assertEquals(Types.FloatType.get(), literal.dataType());

    literal = doubleLiteral(Double.valueOf("1.234"));
    Assertions.assertEquals(1.234d, literal.value());
    Assertions.assertEquals(Types.DoubleType.get(), literal.dataType());

    literal = dateLiteral(LocalDate.parse("2020-01-01"));
    Assertions.assertEquals(LocalDate.of(2020, 1, 1), literal.value());
    Assertions.assertEquals(Types.DateType.get(), literal.dataType());

    literal = timeLiteral(LocalTime.parse("12:34:56"));
    Assertions.assertEquals(LocalTime.of(12, 34, 56), literal.value());
    Assertions.assertEquals(Types.TimeType.get(), literal.dataType());

    literal = timestampLiteral(LocalDateTime.parse("2020-01-01T12:34:56"));
    Assertions.assertEquals(LocalDateTime.of(2020, 1, 1, 12, 34, 56), literal.value());
    Assertions.assertEquals(Types.TimestampType.withoutTimeZone(), literal.dataType());

    literal = stringLiteral("hello");
    Assertions.assertEquals("hello", literal.value());
    Assertions.assertEquals(Types.StringType.get(), literal.dataType());

    Assertions.assertEquals(Literals.NULL, Literals.of(null, Types.NullType.get()));

    literal = decimalLiteral(Decimal.of("0.00"));
    Assertions.assertEquals(Decimal.of(new BigDecimal("0.00")), literal.value());
    Assertions.assertEquals(Types.DecimalType.of(2, 2), literal.dataType());
  }
}
