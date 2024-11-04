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

package org.apache.gravitino.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

public class TestTypeConverter {

  @Test
  public void testConvertBasicTypes() {
    assertEquals(Types.NullType.get(), TypeConverter.convert("null"));
    assertEquals(Types.BooleanType.get(), TypeConverter.convert("boolean"));
    assertEquals(Types.ByteType.get(), TypeConverter.convert("byte"));
    assertEquals(Types.ByteType.unsigned(), TypeConverter.convert("ubyte"));
    assertEquals(Types.ShortType.get(), TypeConverter.convert("short"));
    assertEquals(Types.ShortType.unsigned(), TypeConverter.convert("ushort"));
    assertEquals(Types.IntegerType.get(), TypeConverter.convert("integer"));
    assertEquals(Types.IntegerType.unsigned(), TypeConverter.convert("uinteger"));
    assertEquals(Types.LongType.get(), TypeConverter.convert("long"));
    assertEquals(Types.LongType.unsigned(), TypeConverter.convert("ulong"));
    assertEquals(Types.FloatType.get(), TypeConverter.convert("float"));
    assertEquals(Types.DoubleType.get(), TypeConverter.convert("double"));
    assertEquals(Types.DateType.get(), TypeConverter.convert("date"));
    assertEquals(Types.TimeType.get(), TypeConverter.convert("time"));
    assertEquals(Types.TimestampType.withoutTimeZone(), TypeConverter.convert("timestamp"));
    assertEquals(Types.TimestampType.withTimeZone(), TypeConverter.convert("tztimestamp"));
    assertEquals(Types.IntervalYearType.get(), TypeConverter.convert("intervalyear"));
    assertEquals(Types.IntervalDayType.get(), TypeConverter.convert("intervalday"));
    assertEquals(Types.UUIDType.get(), TypeConverter.convert("uuid"));
    assertEquals(Types.StringType.get(), TypeConverter.convert("string"));
    assertEquals(Types.BinaryType.get(), TypeConverter.convert("binary"));
  }

  @Test
  public void testConvertBasicTypesCaseInsensitive() {
    assertEquals(Types.BooleanType.get(), TypeConverter.convert("BOOLEAN"));
    assertEquals(Types.StringType.get(), TypeConverter.convert("STRING"));
  }

  @Test
  public void testConvertUnsupportedType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          TypeConverter.convert("unsupportedType");
        });
  }

  @Test
  public void testConvertFixedVarcharAndChar() {
    assertEquals(Types.FixedType.of(10), TypeConverter.convert("fixed", 10));
    assertEquals(Types.VarCharType.of(20), TypeConverter.convert("varchar", 20));
    assertEquals(Types.FixedCharType.of(30), TypeConverter.convert("char", 30));
  }

  @Test
  public void testConvertFixedVarcharAndCharUnsupportedType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          TypeConverter.convert("unsupportedType", 10);
        });
  }

  @Test
  public void testConvertDecimal() {
    assertEquals(Types.DecimalType.of(10, 5), TypeConverter.convert("decimal", 10, 5));
  }

  @Test
  public void testConvertDecimalUnsupportedType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          TypeConverter.convert("unsupportedType", 10, 5);
        });
  }
}
