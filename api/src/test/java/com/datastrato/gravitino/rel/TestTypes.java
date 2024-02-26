/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTypes {

  @Test
  public void testPrimitiveTypes() {
    Types.BooleanType booleanType = Types.BooleanType.get();
    Assertions.assertEquals(Type.Name.BOOLEAN, booleanType.name());
    // asset that the type is a singleton
    Assertions.assertSame(booleanType, Types.BooleanType.get());
    Assertions.assertEquals("boolean", booleanType.simpleString());

    Types.NullType nullType = Types.NullType.get();
    Assertions.assertEquals(Type.Name.NULL, nullType.name());
    Assertions.assertSame(nullType, Types.NullType.get());
    Assertions.assertEquals("null", nullType.simpleString());

    Types.ByteType byteType = Types.ByteType.get();
    Assertions.assertEquals(Type.Name.BYTE, byteType.name());
    Assertions.assertSame(byteType, Types.ByteType.get());
    Assertions.assertEquals("byte", byteType.simpleString());

    Types.ShortType shortType = Types.ShortType.get();
    Assertions.assertEquals(Type.Name.SHORT, shortType.name());
    Assertions.assertSame(shortType, Types.ShortType.get());
    Assertions.assertEquals("short", shortType.simpleString());

    Types.IntegerType integerType = Types.IntegerType.get();
    Assertions.assertEquals(Type.Name.INTEGER, integerType.name());
    Assertions.assertSame(integerType, Types.IntegerType.get());
    Assertions.assertEquals("integer", integerType.simpleString());

    Types.LongType longType = Types.LongType.get();
    Assertions.assertEquals(Type.Name.LONG, longType.name());
    Assertions.assertSame(longType, Types.LongType.get());
    Assertions.assertEquals("long", longType.simpleString());

    Types.FloatType floatType = Types.FloatType.get();
    Assertions.assertEquals(Type.Name.FLOAT, floatType.name());
    Assertions.assertSame(floatType, Types.FloatType.get());
    Assertions.assertEquals("float", floatType.simpleString());

    Types.DoubleType doubleType = Types.DoubleType.get();
    Assertions.assertEquals(Type.Name.DOUBLE, doubleType.name());
    Assertions.assertSame(doubleType, Types.DoubleType.get());
    Assertions.assertEquals("double", doubleType.simpleString());

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> Types.DecimalType.of(40, 0));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Decimals with precision larger than 38 are not supported"));

    exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> Types.DecimalType.of(0, 40));
    Assertions.assertTrue(exception.getMessage().contains("Scale cannot be larger than precision"));

    Types.DecimalType decimalType = Types.DecimalType.of(26, 10);
    Assertions.assertEquals(Type.Name.DECIMAL, decimalType.name());
    Assertions.assertEquals(26, decimalType.precision());
    Assertions.assertEquals(10, decimalType.scale());
    Assertions.assertEquals("decimal(26,10)", decimalType.simpleString());
    Assertions.assertEquals(decimalType, Types.DecimalType.of(26, 10));

    Types.DateType dateType = Types.DateType.get();
    Assertions.assertEquals(Type.Name.DATE, dateType.name());
    Assertions.assertSame(dateType, Types.DateType.get());
    Assertions.assertEquals("date", dateType.simpleString());

    Types.TimeType timeType = Types.TimeType.get();
    Assertions.assertEquals(Type.Name.TIME, timeType.name());
    Assertions.assertSame(timeType, Types.TimeType.get());
    Assertions.assertEquals("time", timeType.simpleString());

    Types.TimestampType timestampType = Types.TimestampType.withoutTimeZone();
    Assertions.assertEquals(Type.Name.TIMESTAMP, timestampType.name());
    Assertions.assertSame(timestampType, Types.TimestampType.withoutTimeZone());
    Assertions.assertEquals("timestamp", timestampType.simpleString());
    Assertions.assertFalse(timestampType.hasTimeZone());

    Types.TimestampType timestampTzType = Types.TimestampType.withTimeZone();
    Assertions.assertEquals(Type.Name.TIMESTAMP, timestampTzType.name());
    Assertions.assertSame(timestampTzType, Types.TimestampType.withTimeZone());
    Assertions.assertEquals("timestamp_tz", timestampTzType.simpleString());
    Assertions.assertTrue(timestampTzType.hasTimeZone());

    Types.IntervalYearType intervalYearType = Types.IntervalYearType.get();
    Assertions.assertEquals(Type.Name.INTERVAL_YEAR, intervalYearType.name());
    Assertions.assertSame(intervalYearType, Types.IntervalYearType.get());
    Assertions.assertEquals("interval_year", intervalYearType.simpleString());

    Types.IntervalDayType intervalDayType = Types.IntervalDayType.get();
    Assertions.assertEquals(Type.Name.INTERVAL_DAY, intervalDayType.name());
    Assertions.assertSame(intervalDayType, Types.IntervalDayType.get());
    Assertions.assertEquals("interval_day", intervalDayType.simpleString());

    Types.StringType stringType = Types.StringType.get();
    Assertions.assertEquals(Type.Name.STRING, stringType.name());
    Assertions.assertSame(stringType, Types.StringType.get());
    Assertions.assertEquals("string", stringType.simpleString());

    Types.UUIDType uuidType = Types.UUIDType.get();
    Assertions.assertEquals(Type.Name.UUID, uuidType.name());
    Assertions.assertSame(uuidType, Types.UUIDType.get());
    Assertions.assertEquals("uuid", uuidType.simpleString());

    Types.FixedType fixedType = Types.FixedType.of(10);
    Assertions.assertEquals(Type.Name.FIXED, fixedType.name());
    Assertions.assertEquals(10, fixedType.length());
    Assertions.assertEquals("fixed(10)", fixedType.simpleString());
    Assertions.assertEquals(fixedType, Types.FixedType.of(10));

    Types.VarCharType varcharType = Types.VarCharType.of(20);
    Assertions.assertEquals(Type.Name.VARCHAR, varcharType.name());
    Assertions.assertEquals(20, varcharType.length());
    Assertions.assertEquals("varchar(20)", varcharType.simpleString());
    Assertions.assertEquals(varcharType, Types.VarCharType.of(20));

    Types.FixedCharType fixedCharType = Types.FixedCharType.of(30);
    Assertions.assertEquals(Type.Name.FIXEDCHAR, fixedCharType.name());
    Assertions.assertEquals(30, fixedCharType.length());
    Assertions.assertEquals("char(30)", fixedCharType.simpleString());
    Assertions.assertEquals(fixedCharType, Types.FixedCharType.of(30));

    Types.BinaryType binaryType = Types.BinaryType.get();
    Assertions.assertEquals(Type.Name.BINARY, binaryType.name());
    Assertions.assertSame(binaryType, Types.BinaryType.get());
  }

  @Test
  public void testComplexTypes() {
    Types.StructType structType =
        Types.StructType.of(
            Types.StructType.Field.notNullField("id", Types.IntegerType.get()),
            Types.StructType.Field.notNullField("name", Types.StringType.get(), "name field"),
            Types.StructType.Field.nullableField("is_studying", Types.BooleanType.get()),
            Types.StructType.Field.nullableField("score", Types.DoubleType.get(), "score field"));
    Assertions.assertEquals(Type.Name.STRUCT, structType.name());
    Assertions.assertEquals(4, structType.fields().length);
    Assertions.assertEquals(
        "struct<id: integer NOT NULL COMMENT ,"
            + "name: string NOT NULL COMMENT 'name field',"
            + "is_studying: boolean NULL COMMENT ,"
            + "score: double NULL COMMENT 'score field'>",
        structType.simpleString());
    Assertions.assertEquals(structType, Types.StructType.of(structType.fields()));

    Types.ListType listType = Types.ListType.nullable(Types.IntegerType.get());
    Assertions.assertEquals(Type.Name.LIST, listType.name());
    Assertions.assertTrue(listType.elementNullable());
    Assertions.assertEquals(Types.IntegerType.get(), listType.elementType());
    Assertions.assertEquals("list<integer>", listType.simpleString());
    Assertions.assertEquals(listType, Types.ListType.nullable(Types.IntegerType.get()));

    Types.MapType mapType =
        Types.MapType.valueNullable(Types.IntegerType.get(), Types.StringType.get());
    Assertions.assertEquals(Type.Name.MAP, mapType.name());
    Assertions.assertEquals(Types.IntegerType.get(), mapType.keyType());
    Assertions.assertEquals(Types.StringType.get(), mapType.valueType());
    Assertions.assertEquals("map<integer,string>", mapType.simpleString());
    Assertions.assertEquals(
        mapType, Types.MapType.valueNullable(Types.IntegerType.get(), Types.StringType.get()));

    Types.UnionType unionType =
        Types.UnionType.of(
            Types.IntegerType.get(), Types.StringType.get(), Types.BooleanType.get());
    Assertions.assertEquals(Type.Name.UNION, unionType.name());
    Assertions.assertEquals(3, unionType.types().length);
    Assertions.assertEquals("union<integer,string,boolean>", unionType.simpleString());
    Assertions.assertEquals(unionType, Types.UnionType.of(unionType.types()));
  }

  @Test
  public void testUnparsedType() {
    Types.UnparsedType unparsedType = Types.UnparsedType.of("bit");
    Assertions.assertEquals(Type.Name.UNPARSED, unparsedType.name());
    Assertions.assertEquals("unparsed(bit)", unparsedType.simpleString());
    Assertions.assertEquals("bit", unparsedType.unparsedType());
    Assertions.assertEquals(unparsedType, Types.UnparsedType.of("bit"));
  }
}
