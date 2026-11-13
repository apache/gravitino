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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  // toGravitino — complex types
  // -------------------------------------------------------------------------

  @Test
  void testArrayType() {
    assertEquals(
        Types.ListType.nullable(Types.StringType.get()), CONVERTER.toGravitino("array<string>"));
    assertEquals(
        Types.ListType.nullable(Types.LongType.get()), CONVERTER.toGravitino("array<bigint>"));
  }

  @Test
  void testMapType() {
    assertEquals(
        Types.MapType.valueNullable(Types.StringType.get(), Types.IntegerType.get()),
        CONVERTER.toGravitino("map<string,int>"));
  }

  @Test
  void testStructType() {
    Types.StructType expected =
        Types.StructType.of(
            Types.StructType.Field.nullableField("id", Types.LongType.get()),
            Types.StructType.Field.nullableField("name", Types.StringType.get()));
    assertEquals(expected, CONVERTER.toGravitino("struct<id:bigint,name:string>"));
  }

  @Test
  void testUnionType() {
    assertEquals(
        Types.UnionType.of(Types.IntegerType.get(), Types.StringType.get()),
        CONVERTER.toGravitino("uniontype<int,string>"));
  }

  @Test
  void testNestedComplexType() {
    // array<map<string,int>>
    Type expected =
        Types.ListType.nullable(
            Types.MapType.valueNullable(Types.StringType.get(), Types.IntegerType.get()));
    assertEquals(expected, CONVERTER.toGravitino("array<map<string,int>>"));
  }

  // -------------------------------------------------------------------------
  // toGravitino — unknown types → ExternalType
  // -------------------------------------------------------------------------

  @Test
  void testUnknownTypeBecomesExternalType() {
    assertInstanceOf(Types.ExternalType.class, CONVERTER.toGravitino("unknown_custom_type"));
  }

  @Test
  void testBlankInputThrows() {
    assertThrows(IllegalArgumentException.class, () -> CONVERTER.toGravitino(null));
    assertThrows(IllegalArgumentException.class, () -> CONVERTER.toGravitino(""));
    assertThrows(IllegalArgumentException.class, () -> CONVERTER.toGravitino("   "));
  }

  @Test
  void testInvalidKnownTypeThrows() {
    // Malformed parameterized types should throw, not fall back to ExternalType
    assertThrows(IllegalArgumentException.class, () -> CONVERTER.toGravitino("char(abc)"));
    assertThrows(IllegalArgumentException.class, () -> CONVERTER.toGravitino("varchar(xyz)"));
    assertThrows(IllegalArgumentException.class, () -> CONVERTER.toGravitino("decimal(p,s)"));
    // Malformed complex types should throw
    assertThrows(IllegalArgumentException.class, () -> CONVERTER.toGravitino("map<string>"));
    assertThrows(
        IllegalArgumentException.class, () -> CONVERTER.toGravitino("struct<no_colon_field>"));
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
  void testRoundTripComplexTypes() {
    roundTrip("array<string>", Types.ListType.nullable(Types.StringType.get()));
    roundTrip(
        "map<string,int>",
        Types.MapType.valueNullable(Types.StringType.get(), Types.IntegerType.get()));
    roundTrip(
        "struct<id:bigint,name:string>",
        Types.StructType.of(
            Types.StructType.Field.nullableField("id", Types.LongType.get()),
            Types.StructType.Field.nullableField("name", Types.StringType.get())));
    roundTrip(
        "uniontype<int,string>",
        Types.UnionType.of(Types.IntegerType.get(), Types.StringType.get()));
  }

  @Test
  void testRoundTripNestedComplexType() {
    roundTrip(
        "array<map<string,int>>",
        Types.ListType.nullable(
            Types.MapType.valueNullable(Types.StringType.get(), Types.IntegerType.get())));
  }

  @Test
  void testFromGravitinoExternalType() {
    String raw = "unknown_custom_type";
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

  @Test
  void testStructFieldNamesKeepTheirCase() {
    String glueType = "struct<Outer:struct<InNer:int>,lowx:string>";
    Types.StructType struct = (Types.StructType) CONVERTER.toGravitino(glueType);

    assertEquals("Outer", struct.fields()[0].name());
    assertEquals("InNer", ((Types.StructType) struct.fields()[0].type()).fields()[0].name());
    assertEquals(glueType, CONVERTER.fromGravitino(struct));
  }

  @Test
  void testStructFieldNameThatBreaksTypeStringIsRefused() {
    for (String name : new String[] {"a:b", "x,y", "a<b", " ", "a(", "b)", " a", "a "}) {
      Types.StructType struct =
          Types.StructType.of(Types.StructType.Field.nullableField(name, Types.IntegerType.get()));
      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> CONVERTER.fromGravitino(struct));
      assertTrue(e.getMessage().contains("Struct field name"), e.getMessage());
    }
  }

  @Test
  void testBareDecimalAndDoublePrecision() {
    assertEquals(Types.DecimalType.of(10, 0), CONVERTER.toGravitino("decimal"));
    assertEquals(Types.DecimalType.of(10, 0), CONVERTER.toGravitino("DECIMAL"));
    assertEquals(
        Types.ListType.nullable(Types.DecimalType.of(10, 0)),
        CONVERTER.toGravitino("array<DECIMAL>"));
    assertEquals(Types.DoubleType.get(), CONVERTER.toGravitino("double precision"));
    assertEquals(
        Types.MapType.valueNullable(Types.StringType.get(), Types.DoubleType.get()),
        CONVERTER.toGravitino("map<string,DOUBLE PRECISION>"));
  }

  @Test
  void testUnsignedIntegersAreRefused() {
    for (Type type :
        new Type[] {
          Types.ByteType.unsigned(),
          Types.ShortType.unsigned(),
          Types.IntegerType.unsigned(),
          Types.LongType.unsigned()
        }) {
      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> CONVERTER.fromGravitino(type));
      assertTrue(e.getMessage().contains(type.simpleString()), e.getMessage());
    }
  }

  @Test
  void testCharAndVarcharLengthsWithinHiveLimits() {
    assertEquals("char(1)", CONVERTER.fromGravitino(Types.FixedCharType.of(1)));
    assertEquals("char(255)", CONVERTER.fromGravitino(Types.FixedCharType.of(255)));
    assertEquals("varchar(1)", CONVERTER.fromGravitino(Types.VarCharType.of(1)));
    assertEquals("varchar(65535)", CONVERTER.fromGravitino(Types.VarCharType.of(65535)));
  }

  @Test
  void testCharAndVarcharLengthsOutsideHiveLimitsAreRefused() {
    for (Type type :
        new Type[] {
          Types.FixedCharType.of(0),
          Types.FixedCharType.of(256),
          Types.VarCharType.of(0),
          Types.VarCharType.of(65536),
          Types.ListType.nullable(Types.FixedCharType.of(256)),
          Types.MapType.valueNullable(Types.StringType.get(), Types.VarCharType.of(65536)),
          Types.StructType.of(Types.StructType.Field.nullableField("a", Types.VarCharType.of(0)))
        }) {
      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> CONVERTER.fromGravitino(type));
      assertTrue(e.getMessage().contains("length must be in [1, "), e.getMessage());
    }
  }

  @Test
  void testNestedNotNullIsRefused() {
    for (Type type :
        new Type[] {
          Types.ListType.notNull(Types.StringType.get()),
          Types.MapType.valueNotNull(Types.StringType.get(), Types.LongType.get()),
          Types.StructType.of(Types.StructType.Field.notNullField("a", Types.LongType.get())),
          Types.ListType.nullable(
              Types.StructType.of(Types.StructType.Field.notNullField("a", Types.LongType.get())))
        }) {
      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> CONVERTER.fromGravitino(type));
      assertTrue(
          e.getMessage().contains("does not support NOT NULL on nested fields"), e.getMessage());
    }
  }

  @Test
  void testUnsupportedTypeMessageUsesTypeSyntax() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> CONVERTER.fromGravitino(Types.FixedType.of(16)));
    assertTrue(e.getMessage().contains("fixed(16)"), e.getMessage());
    assertFalse(e.getMessage().contains("@"), e.getMessage());
  }
}
