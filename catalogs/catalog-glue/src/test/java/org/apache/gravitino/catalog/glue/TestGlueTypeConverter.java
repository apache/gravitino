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
package org.apache.gravitino.catalog.glue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link GlueTypeConverter}. */
class TestGlueTypeConverter {

  private static final GlueTypeConverter CONVERTER = new GlueTypeConverter();

  // -------------------------------------------------------------------------
  // toGravitino — primitive types
  // -------------------------------------------------------------------------

  @Test
  void testPrimitiveTypes() {
    assertEquals(Types.BooleanType.get(), CONVERTER.toGravitino("boolean"));
    assertEquals(Types.ByteType.get(), CONVERTER.toGravitino("tinyint"));
    assertEquals(Types.ShortType.get(), CONVERTER.toGravitino("smallint"));
    assertEquals(Types.IntegerType.get(), CONVERTER.toGravitino("int"));
    assertEquals(Types.IntegerType.get(), CONVERTER.toGravitino("integer"));
    assertEquals(Types.LongType.get(), CONVERTER.toGravitino("bigint"));
    assertEquals(Types.FloatType.get(), CONVERTER.toGravitino("float"));
    assertEquals(Types.DoubleType.get(), CONVERTER.toGravitino("double"));
    assertEquals(Types.StringType.get(), CONVERTER.toGravitino("string"));
    assertEquals(Types.DateType.get(), CONVERTER.toGravitino("date"));
    assertEquals(Types.TimestampType.withoutTimeZone(), CONVERTER.toGravitino("timestamp"));
    assertEquals(Types.BinaryType.get(), CONVERTER.toGravitino("binary"));
    assertEquals(Types.IntervalYearType.get(), CONVERTER.toGravitino("interval_year_month"));
    assertEquals(Types.IntervalDayType.get(), CONVERTER.toGravitino("interval_day_time"));
  }

  @Test
  void testCaseInsensitive() {
    assertEquals(Types.LongType.get(), CONVERTER.toGravitino("BIGINT"));
    assertEquals(Types.StringType.get(), CONVERTER.toGravitino("STRING"));
  }

  // -------------------------------------------------------------------------
  // toGravitino — parameterised types
  // -------------------------------------------------------------------------

  @Test
  void testCharType() {
    assertEquals(Types.FixedCharType.of(10), CONVERTER.toGravitino("char(10)"));
    assertEquals(Types.FixedCharType.of(1), CONVERTER.toGravitino("char(1)"));
  }

  @Test
  void testVarcharType() {
    assertEquals(Types.VarCharType.of(255), CONVERTER.toGravitino("varchar(255)"));
    assertEquals(Types.VarCharType.of(65535), CONVERTER.toGravitino("varchar(65535)"));
  }

  @Test
  void testDecimalType() {
    assertEquals(Types.DecimalType.of(10, 2), CONVERTER.toGravitino("decimal(10,2)"));
    assertEquals(Types.DecimalType.of(38, 18), CONVERTER.toGravitino("decimal(38, 18)"));
    assertEquals(Types.DecimalType.of(5, 0), CONVERTER.toGravitino("decimal(5)"));
  }

  // -------------------------------------------------------------------------
  // toGravitino — complex / unknown types → ExternalType
  // -------------------------------------------------------------------------

  @Test
  void testComplexTypesBecomesExternalType() {
    assertInstanceOf(Types.ExternalType.class, CONVERTER.toGravitino("array<string>"));
    assertInstanceOf(Types.ExternalType.class, CONVERTER.toGravitino("map<string,int>"));
    assertInstanceOf(
        Types.ExternalType.class, CONVERTER.toGravitino("struct<id:bigint,name:string>"));
    assertInstanceOf(Types.ExternalType.class, CONVERTER.toGravitino("uniontype<int,string>"));
    assertInstanceOf(Types.ExternalType.class, CONVERTER.toGravitino("unknown_custom_type"));
  }

  @Test
  void testExternalTypePreservesOriginalString() {
    String rawType = "array<map<string,int>>";
    Type type = CONVERTER.toGravitino(rawType);
    assertInstanceOf(Types.ExternalType.class, type);
    assertEquals(rawType, ((Types.ExternalType) type).catalogString());
  }

  @Test
  void testNullInputThrows() {
    assertThrows(IllegalArgumentException.class, () -> CONVERTER.toGravitino(null));
  }

  @Test
  void testEmptyInputBecomesExternalType() {
    assertInstanceOf(Types.ExternalType.class, CONVERTER.toGravitino(""));
  }

  // -------------------------------------------------------------------------
  // fromGravitino — round-trip
  // -------------------------------------------------------------------------

  @Test
  void testRoundTripPrimitives() {
    roundTrip("boolean", Types.BooleanType.get());
    roundTrip("tinyint", Types.ByteType.get());
    roundTrip("smallint", Types.ShortType.get());
    roundTrip("int", Types.IntegerType.get());
    roundTrip("bigint", Types.LongType.get());
    roundTrip("float", Types.FloatType.get());
    roundTrip("double", Types.DoubleType.get());
    roundTrip("string", Types.StringType.get());
    roundTrip("date", Types.DateType.get());
    roundTrip("timestamp", Types.TimestampType.withoutTimeZone());
    roundTrip("binary", Types.BinaryType.get());
    roundTrip("interval_year_month", Types.IntervalYearType.get());
    roundTrip("interval_day_time", Types.IntervalDayType.get());
  }

  @Test
  void testRoundTripParameterised() {
    assertEquals("char(10)", CONVERTER.fromGravitino(Types.FixedCharType.of(10)));
    assertEquals("varchar(255)", CONVERTER.fromGravitino(Types.VarCharType.of(255)));
    assertEquals("decimal(10,2)", CONVERTER.fromGravitino(Types.DecimalType.of(10, 2)));
  }

  @Test
  void testFromGravitinoExternalType() {
    String raw = "array<string>";
    assertEquals(raw, CONVERTER.fromGravitino(Types.ExternalType.of(raw)));
  }

  @Test
  void testFromGravitinoTimestampWithTimeZoneThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CONVERTER.fromGravitino(Types.TimestampType.withTimeZone()));
  }

  @Test
  void testFromGravitinoUnsupportedTypeThrows() {
    assertThrows(
        IllegalArgumentException.class, () -> CONVERTER.fromGravitino(Types.NullType.get()));
  }

  // -------------------------------------------------------------------------
  // helpers
  // -------------------------------------------------------------------------

  private static void roundTrip(String glueType, Type gravitinoType) {
    assertEquals(gravitinoType, CONVERTER.toGravitino(glueType));
    assertEquals(glueType, CONVERTER.fromGravitino(gravitinoType));
  }
}
