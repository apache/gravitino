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
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link HologresTypeConverter}. */
public class TestHologresTypeConverter {

  private final HologresTypeConverter converter = new HologresTypeConverter();

  @Test
  public void testBooleanType() {
    // Test toGravitino
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("bool");
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.BooleanType.get(), gravitinoType);

    // Test fromGravitino
    String hologresType = converter.fromGravitino(Types.BooleanType.get());
    Assertions.assertEquals("bool", hologresType);
  }

  @Test
  public void testShortType() {
    // Test toGravitino
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("int2");
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.ShortType.get(), gravitinoType);

    // Test fromGravitino
    String hologresType = converter.fromGravitino(Types.ShortType.get());
    Assertions.assertEquals("int2", hologresType);
  }

  @Test
  public void testIntegerType() {
    // Test toGravitino
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("int4");
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.IntegerType.get(), gravitinoType);

    // Test fromGravitino
    String hologresType = converter.fromGravitino(Types.IntegerType.get());
    Assertions.assertEquals("int4", hologresType);
  }

  @Test
  public void testLongType() {
    // Test toGravitino
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("int8");
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.LongType.get(), gravitinoType);

    // Test fromGravitino
    String hologresType = converter.fromGravitino(Types.LongType.get());
    Assertions.assertEquals("int8", hologresType);
  }

  @Test
  public void testFloatType() {
    // Test toGravitino
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("float4");
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.FloatType.get(), gravitinoType);

    // Test fromGravitino
    String hologresType = converter.fromGravitino(Types.FloatType.get());
    Assertions.assertEquals("float4", hologresType);
  }

  @Test
  public void testDoubleType() {
    // Test toGravitino
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("float8");
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.DoubleType.get(), gravitinoType);

    // Test fromGravitino
    String hologresType = converter.fromGravitino(Types.DoubleType.get());
    Assertions.assertEquals("float8", hologresType);
  }

  @Test
  public void testDateType() {
    // Test toGravitino
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("date");
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.DateType.get(), gravitinoType);

    // Test fromGravitino
    String hologresType = converter.fromGravitino(Types.DateType.get());
    Assertions.assertEquals("date", hologresType);
  }

  @Test
  public void testTimeType() {
    // Test toGravitino
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("time");
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.TimeType.get(), gravitinoType);

    // Test fromGravitino
    String hologresType = converter.fromGravitino(Types.TimeType.get());
    Assertions.assertEquals("time", hologresType);
  }

  @Test
  public void testTimestampType() {
    // Test toGravitino without timezone
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("timestamp");
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.TimestampType.withoutTimeZone(), gravitinoType);

    // Test toGravitino with timezone
    typeBean = new JdbcTypeConverter.JdbcTypeBean("timestamptz");
    gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.TimestampType.withTimeZone(), gravitinoType);

    // Test fromGravitino without timezone
    String hologresType = converter.fromGravitino(Types.TimestampType.withoutTimeZone());
    Assertions.assertEquals("timestamp", hologresType);

    // Test fromGravitino with timezone
    hologresType = converter.fromGravitino(Types.TimestampType.withTimeZone());
    Assertions.assertEquals("timestamptz", hologresType);

    // Hologres stores timestamp without time zone with microsecond precision.
    hologresType = converter.fromGravitino(Types.TimestampType.withoutTimeZone(6));
    Assertions.assertEquals("timestamp", hologresType);

    // Hologres stores timestamp with time zone with millisecond precision.
    hologresType = converter.fromGravitino(Types.TimestampType.withTimeZone(3));
    Assertions.assertEquals("timestamptz", hologresType);
  }

  @Test
  public void testTimestampMetadataIsNormalizedToNativePrecision() {
    JdbcTypeConverter.JdbcTypeBean typeBean =
        new JdbcTypeConverter.JdbcTypeBean(HologresTypeConverter.TIMESTAMP);
    typeBean.setDatetimePrecision(9);
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(HologresTypeConverter.MAX_TIMESTAMP_PRECISION),
        converter.toGravitino(typeBean));

    typeBean = new JdbcTypeConverter.JdbcTypeBean(HologresTypeConverter.TIMESTAMP_TZ);
    typeBean.setDatetimePrecision(6);
    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(HologresTypeConverter.MAX_TIMESTAMPTZ_PRECISION),
        converter.toGravitino(typeBean));
  }

  @Test
  public void testRejectNanosecondTimestampTypes() {
    Type[] nanosecondTypes = {
      Types.TimestampType.withoutTimeZone(9), Types.TimestampType.withTimeZone(9)
    };

    for (Type type : nanosecondTypes) {
      IllegalArgumentException exception =
          Assertions.assertThrows(
              IllegalArgumentException.class, () -> converter.fromGravitino(type));
      Assertions.assertTrue(exception.getMessage().contains("cannot preserve Gravitino type"));
    }
  }

  @Test
  public void testDecimalType() {
    // Test toGravitino
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("numeric");
    typeBean.setColumnSize(10);
    typeBean.setScale(2);
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.DecimalType.of(10, 2), gravitinoType);

    // Test fromGravitino
    String hologresType = converter.fromGravitino(Types.DecimalType.of(10, 2));
    Assertions.assertEquals("numeric(10,2)", hologresType);
  }

  @Test
  public void testVarCharType() {
    // Test toGravitino
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("varchar");
    typeBean.setColumnSize(255);
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.VarCharType.of(255), gravitinoType);

    // Test fromGravitino
    String hologresType = converter.fromGravitino(Types.VarCharType.of(255));
    Assertions.assertEquals("varchar(255)", hologresType);
  }

  @Test
  public void testFixedCharType() {
    // Test toGravitino
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("bpchar");
    typeBean.setColumnSize(10);
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.FixedCharType.of(10), gravitinoType);

    // Test fromGravitino
    String hologresType = converter.fromGravitino(Types.FixedCharType.of(10));
    Assertions.assertEquals("bpchar(10)", hologresType);
  }

  @Test
  public void testTextType() {
    // Test toGravitino
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("text");
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.StringType.get(), gravitinoType);

    // Test fromGravitino
    String hologresType = converter.fromGravitino(Types.StringType.get());
    Assertions.assertEquals("text", hologresType);
  }

  @Test
  public void testBinaryType() {
    // Test toGravitino
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("bytea");
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.BinaryType.get(), gravitinoType);

    // Test fromGravitino
    String hologresType = converter.fromGravitino(Types.BinaryType.get());
    Assertions.assertEquals("bytea", hologresType);
  }

  @Test
  public void testArrayType() {
    // Test toGravitino for int4 array
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("_int4");
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.ListType.of(Types.IntegerType.get(), false), gravitinoType);

    // Test fromGravitino for int4 array
    String hologresType =
        converter.fromGravitino(Types.ListType.of(Types.IntegerType.get(), false));
    Assertions.assertEquals("int4[]", hologresType);

    // Test toGravitino for text array
    typeBean = new JdbcTypeConverter.JdbcTypeBean("_text");
    gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.ListType.of(Types.StringType.get(), false), gravitinoType);

    // Test fromGravitino for text array
    hologresType = converter.fromGravitino(Types.ListType.of(Types.StringType.get(), false));
    Assertions.assertEquals("text[]", hologresType);
  }

  @Test
  public void testExternalType() {
    // Test toGravitino for unknown type
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("json");
    Type gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.ExternalType.of("json"), gravitinoType);

    // Test fromGravitino for external type
    String hologresType = converter.fromGravitino(Types.ExternalType.of("jsonb"));
    Assertions.assertEquals("jsonb", hologresType);

    typeBean = new JdbcTypeConverter.JdbcTypeBean("jsonb");
    gravitinoType = converter.toGravitino(typeBean);
    Assertions.assertEquals(Types.ExternalType.of("jsonb"), gravitinoType);
  }

  @Test
  public void testRejectVariantType() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> converter.fromGravitino(Types.VariantType.get()));

    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Hologres JSON and JSONB do not preserve Gravitino Variant semantics"));
  }

  @Test
  public void testRejectUnknownType() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> converter.fromGravitino(Types.NullType.get()));

    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Hologres table columns cannot represent Gravitino Unknown (NullType)"));

    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("unknown");
    Assertions.assertEquals(Types.ExternalType.of("unknown"), converter.toGravitino(typeBean));
  }

  @Test
  public void testRejectGeometryType() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> converter.fromGravitino(Types.GeometryType.of("EPSG:3857")));

    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(
                "Hologres PostGIS geometry metadata does not preserve Gravitino Geometry CRS semantics"));

    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean("geometry");
    Assertions.assertEquals(Types.ExternalType.of("geometry"), converter.toGravitino(typeBean));
  }

  @Test
  public void testNullableArrayThrowsException() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> converter.fromGravitino(Types.ListType.of(Types.IntegerType.get(), true)));
  }

  @Test
  public void testMultidimensionalArrayThrowsException() {
    Types.ListType nestedList =
        Types.ListType.of(Types.ListType.of(Types.IntegerType.get(), false), false);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> converter.fromGravitino(nestedList));
  }
}
