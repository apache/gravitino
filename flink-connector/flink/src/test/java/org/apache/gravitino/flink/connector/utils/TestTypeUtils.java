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

package org.apache.gravitino.flink.connector.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.UnresolvedUserDefinedType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTypeUtils {

  @Test
  public void testToGravitinoType() {
    Assertions.assertEquals(
        Types.StringType.get(), TypeUtils.toGravitinoType(new VarCharType(Integer.MAX_VALUE)));
    Assertions.assertEquals(Types.DoubleType.get(), TypeUtils.toGravitinoType(new DoubleType()));
    Assertions.assertEquals(Types.IntegerType.get(), TypeUtils.toGravitinoType(new IntType()));
    Assertions.assertEquals(Types.LongType.get(), TypeUtils.toGravitinoType(new BigIntType()));
    Assertions.assertEquals(
        Types.FixedCharType.of(10), TypeUtils.toGravitinoType(new CharType(10)));
    Assertions.assertEquals(Types.BooleanType.get(), TypeUtils.toGravitinoType(new BooleanType()));
    Assertions.assertEquals(Types.FixedType.of(10), TypeUtils.toGravitinoType(new BinaryType(10)));
    Assertions.assertEquals(Types.ByteType.get(), TypeUtils.toGravitinoType(new TinyIntType()));
    Assertions.assertEquals(Types.DateType.get(), TypeUtils.toGravitinoType(new DateType()));
    Assertions.assertEquals(Types.BinaryType.get(), TypeUtils.toGravitinoType(new VarBinaryType()));
    Assertions.assertEquals(
        Types.DecimalType.of(10, 3), TypeUtils.toGravitinoType(new DecimalType(10, 3)));
    Assertions.assertEquals(Types.ByteType.get(), TypeUtils.toGravitinoType(new TinyIntType()));
    Assertions.assertEquals(Types.ShortType.get(), TypeUtils.toGravitinoType(new SmallIntType()));
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(6), TypeUtils.toGravitinoType(new TimestampType()));
    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(6), TypeUtils.toGravitinoType(new ZonedTimestampType()));
    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(6),
        TypeUtils.toGravitinoType(new LocalZonedTimestampType()));
    Assertions.assertEquals(Types.TimeType.of(0), TypeUtils.toGravitinoType(new TimeType()));
    Assertions.assertEquals(
        Types.IntervalDayType.get(),
        TypeUtils.toGravitinoType(
            new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY)));
    Assertions.assertEquals(
        Types.IntervalYearType.get(),
        TypeUtils.toGravitinoType(
            new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR)));
    Assertions.assertEquals(
        Types.ListType.notNull(Types.IntegerType.get()),
        TypeUtils.toGravitinoType(new ArrayType(false, new IntType())));
    Assertions.assertEquals(
        Types.ListType.nullable(Types.IntegerType.get()),
        TypeUtils.toGravitinoType(new ArrayType(true, new IntType())));
    Assertions.assertEquals(
        Types.MapType.of(Types.StringType.get(), Types.IntegerType.get(), true),
        TypeUtils.toGravitinoType(
            new MapType(true, new VarCharType(Integer.MAX_VALUE), new IntType())));
    Assertions.assertEquals(
        Types.MapType.of(Types.StringType.get(), Types.IntegerType.get(), false),
        TypeUtils.toGravitinoType(
            new MapType(false, new VarCharType(Integer.MAX_VALUE), new IntType())));
    Assertions.assertEquals(
        Types.StructType.of(
            Types.StructType.Field.nullableField("a", Types.IntegerType.get()),
            Types.StructType.Field.notNullField("b", Types.IntegerType.get())),
        TypeUtils.toGravitinoType(
            RowType.of(
                true,
                new IntType[] {new IntType(true), new IntType(false)},
                new String[] {"a", "b"})));
    Assertions.assertEquals(Types.NullType.get(), TypeUtils.toGravitinoType(new NullType()));
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            TypeUtils.toGravitinoType(
                new UnresolvedUserDefinedType(UnresolvedIdentifier.of("a", "b", "c"))));
  }

  @Test
  public void testToFlinkType() {
    Assertions.assertEquals(DataTypes.DOUBLE(), TypeUtils.toFlinkType(Types.DoubleType.get()));
    Assertions.assertEquals(DataTypes.STRING(), TypeUtils.toFlinkType(Types.StringType.get()));
    Assertions.assertEquals(DataTypes.INT(), TypeUtils.toFlinkType(Types.IntegerType.get()));
    Assertions.assertEquals(DataTypes.BIGINT(), TypeUtils.toFlinkType(Types.LongType.get()));
    Assertions.assertEquals(DataTypes.SMALLINT(), TypeUtils.toFlinkType(Types.ShortType.get()));
    Assertions.assertEquals(DataTypes.TINYINT(), TypeUtils.toFlinkType(Types.ByteType.get()));
    Assertions.assertEquals(DataTypes.BOOLEAN(), TypeUtils.toFlinkType(Types.BooleanType.get()));
    Assertions.assertEquals(DataTypes.BYTES(), TypeUtils.toFlinkType(Types.BinaryType.get()));
    Assertions.assertEquals(DataTypes.DATE(), TypeUtils.toFlinkType(Types.DateType.get()));
    Assertions.assertEquals(
        DataTypes.DECIMAL(10, 3), TypeUtils.toFlinkType(Types.DecimalType.of(10, 3)));
    Assertions.assertEquals(DataTypes.CHAR(10), TypeUtils.toFlinkType(Types.FixedCharType.of(10)));
    Assertions.assertEquals(DataTypes.BYTES(), TypeUtils.toFlinkType(Types.BinaryType.get()));
    Assertions.assertEquals(DataTypes.BINARY(10), TypeUtils.toFlinkType(Types.FixedType.of(10)));
    Assertions.assertEquals(
        DataTypes.TIMESTAMP(6), TypeUtils.toFlinkType(Types.TimestampType.withoutTimeZone(6)));
    Assertions.assertEquals(
        DataTypes.TIMESTAMP_LTZ(6), TypeUtils.toFlinkType(Types.TimestampType.withTimeZone(6)));
    Assertions.assertEquals(DataTypes.TIME(0), TypeUtils.toFlinkType(Types.TimeType.of(0)));
    Assertions.assertEquals(
        DataTypes.INTERVAL(DataTypes.DAY()), TypeUtils.toFlinkType(Types.IntervalDayType.get()));
    Assertions.assertEquals(
        DataTypes.INTERVAL(DataTypes.YEAR()), TypeUtils.toFlinkType(Types.IntervalYearType.get()));
    Assertions.assertEquals(
        DataTypes.ARRAY(DataTypes.INT().notNull()),
        TypeUtils.toFlinkType(Types.ListType.of(Types.IntegerType.get(), false)));
    Assertions.assertEquals(
        DataTypes.ARRAY(DataTypes.INT().nullable()),
        TypeUtils.toFlinkType(Types.ListType.of(Types.IntegerType.get(), true)));
    Assertions.assertEquals(
        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT().nullable()),
        TypeUtils.toFlinkType(
            Types.MapType.of(Types.StringType.get(), Types.IntegerType.get(), true)));
    Assertions.assertEquals(
        DataTypes.ROW(
            DataTypes.FIELD("a", DataTypes.INT().nullable()),
            DataTypes.FIELD("b", DataTypes.INT().notNull())),
        TypeUtils.toFlinkType(
            Types.StructType.of(
                Types.StructType.Field.nullableField("a", Types.IntegerType.get()),
                Types.StructType.Field.notNullField("b", Types.IntegerType.get()))));
    Assertions.assertEquals(DataTypes.NULL(), TypeUtils.toFlinkType(Types.NullType.get()));
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> TypeUtils.toFlinkType(Types.UnparsedType.of("unknown")));
  }

  @Test
  public void testTimePrecisionConversion() {
    // TIME
    Assertions.assertEquals(Types.TimeType.of(0), TypeUtils.toGravitinoType(new TimeType(0)));
    Assertions.assertEquals(Types.TimeType.of(3), TypeUtils.toGravitinoType(new TimeType(3)));
    Assertions.assertEquals(Types.TimeType.of(6), TypeUtils.toGravitinoType(new TimeType(6)));
    Assertions.assertEquals(Types.TimeType.of(9), TypeUtils.toGravitinoType(new TimeType(9)));

    // Test converting back
    Assertions.assertEquals(DataTypes.TIME(0), TypeUtils.toFlinkType(Types.TimeType.of(0)));
    Assertions.assertEquals(DataTypes.TIME(3), TypeUtils.toFlinkType(Types.TimeType.of(3)));
    Assertions.assertEquals(DataTypes.TIME(6), TypeUtils.toFlinkType(Types.TimeType.of(6)));
    Assertions.assertEquals(DataTypes.TIME(9), TypeUtils.toFlinkType(Types.TimeType.of(9)));
  }

  @Test
  public void testTimestampPrecisionConversion() {
    // TIMESTAMP without timezone
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(0), TypeUtils.toGravitinoType(new TimestampType(0)));
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(3), TypeUtils.toGravitinoType(new TimestampType(3)));
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(6), TypeUtils.toGravitinoType(new TimestampType(6)));
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(9), TypeUtils.toGravitinoType(new TimestampType(9)));

    // TIMESTAMP with timezone (LocalZoned)
    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(0),
        TypeUtils.toGravitinoType(new LocalZonedTimestampType(0)));
    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(3),
        TypeUtils.toGravitinoType(new LocalZonedTimestampType(3)));
    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(6),
        TypeUtils.toGravitinoType(new LocalZonedTimestampType(6)));
    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(9),
        TypeUtils.toGravitinoType(new LocalZonedTimestampType(9)));

    // Test converting back
    Assertions.assertEquals(
        DataTypes.TIMESTAMP(0), TypeUtils.toFlinkType(Types.TimestampType.withoutTimeZone(0)));
    Assertions.assertEquals(
        DataTypes.TIMESTAMP(3), TypeUtils.toFlinkType(Types.TimestampType.withoutTimeZone(3)));
    Assertions.assertEquals(
        DataTypes.TIMESTAMP(6), TypeUtils.toFlinkType(Types.TimestampType.withoutTimeZone(6)));
    Assertions.assertEquals(
        DataTypes.TIMESTAMP(9), TypeUtils.toFlinkType(Types.TimestampType.withoutTimeZone(9)));

    Assertions.assertEquals(
        DataTypes.TIMESTAMP_LTZ(0), TypeUtils.toFlinkType(Types.TimestampType.withTimeZone(0)));
    Assertions.assertEquals(
        DataTypes.TIMESTAMP_LTZ(3), TypeUtils.toFlinkType(Types.TimestampType.withTimeZone(3)));
    Assertions.assertEquals(
        DataTypes.TIMESTAMP_LTZ(6), TypeUtils.toFlinkType(Types.TimestampType.withTimeZone(6)));
    Assertions.assertEquals(
        DataTypes.TIMESTAMP_LTZ(9), TypeUtils.toFlinkType(Types.TimestampType.withTimeZone(9)));
  }

  @Test
  public void testInvalidPrecisionHandling() {
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> TypeUtils.toFlinkType(Types.TimeType.of(10)));

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> TypeUtils.toFlinkType(Types.TimestampType.withoutTimeZone(10)));

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> TypeUtils.toFlinkType(Types.TimestampType.withTimeZone(10)));
  }
}
