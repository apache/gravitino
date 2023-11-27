/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.util;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_UNSUPPORTED_TRINO_DATATYPE;
import static org.testng.Assert.assertEquals;

import com.datastrato.gravitino.rel.types.Types;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.HyperLogLogType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarcharType;
import java.util.Optional;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

public class TestDataTypeTransformer {

  @Test
  public void testGetGravitinoType() {
    assertEquals(
        DataTypeTransformer.getGravitinoType(BooleanType.BOOLEAN), Types.BooleanType.get());

    assertEquals(DataTypeTransformer.getGravitinoType(TinyintType.TINYINT), Types.ByteType.get());
    assertEquals(
        DataTypeTransformer.getGravitinoType(SmallintType.SMALLINT), Types.ShortType.get());
    assertEquals(
        DataTypeTransformer.getGravitinoType(IntegerType.INTEGER), Types.IntegerType.get());
    assertEquals(DataTypeTransformer.getGravitinoType(BigintType.BIGINT), Types.LongType.get());

    assertEquals(DataTypeTransformer.getGravitinoType(RealType.REAL), Types.FloatType.get());
    assertEquals(DataTypeTransformer.getGravitinoType(DoubleType.DOUBLE), Types.DoubleType.get());
    assertEquals(
        DataTypeTransformer.getGravitinoType(DecimalType.createDecimalType(10, 2)),
        Types.DecimalType.of(10, 2));

    assertEquals(
        DataTypeTransformer.getGravitinoType(CharType.createCharType(10)),
        Types.FixedCharType.of(10));
    assertEquals(
        DataTypeTransformer.getGravitinoType(VarcharType.createVarcharType(10)),
        Types.VarCharType.of(10));
    assertEquals(
        DataTypeTransformer.getGravitinoType(VarcharType.createVarcharType(VarcharType.MAX_LENGTH)),
        Types.VarCharType.of(Integer.MAX_VALUE - 1));

    assertEquals(DataTypeTransformer.getGravitinoType(DateType.DATE), Types.DateType.get());
    assertEquals(DataTypeTransformer.getGravitinoType(TimeType.TIME_MILLIS), Types.TimeType.get());
    assertEquals(
        DataTypeTransformer.getGravitinoType(TimestampType.TIMESTAMP_MILLIS),
        Types.TimestampType.withoutTimeZone());

    assertEquals(
        DataTypeTransformer.getGravitinoType(new ArrayType(IntegerType.INTEGER)),
        Types.ListType.nullable(Types.IntegerType.get()));
    assertEquals(
        DataTypeTransformer.getGravitinoType(
            new MapType(SmallintType.SMALLINT, IntegerType.INTEGER, new TypeOperators())),
        Types.MapType.of(Types.ShortType.get(), Types.IntegerType.get(), true));

    assertEquals(
        DataTypeTransformer.getGravitinoType(
            RowType.from(
                ImmutableList.of(
                    new Field(Optional.of("a"), IntegerType.INTEGER),
                    new Field(Optional.of("b"), VarcharType.createVarcharType(10))))),
        Types.StructType.of(
            Types.StructType.Field.nullableField("a", Types.IntegerType.get()),
            Types.StructType.Field.nullableField("b", Types.VarCharType.of(10))));

    assertEquals(DataTypeTransformer.getGravitinoType(UuidType.UUID), Types.UUIDType.get());

    try {
      DataTypeTransformer.getGravitinoType(HyperLogLogType.HYPER_LOG_LOG);
    } catch (TrinoException e) {
      if (e.getErrorCode() != GRAVITINO_UNSUPPORTED_TRINO_DATATYPE.toErrorCode()) {
        throw e;
      }
    }
  }

  @Test
  public void testGetTrinoType() {
    assertEquals(DataTypeTransformer.getTrinoType(Types.BooleanType.get()), BooleanType.BOOLEAN);

    assertEquals(DataTypeTransformer.getTrinoType(Types.ByteType.get()), TinyintType.TINYINT);
    assertEquals(DataTypeTransformer.getTrinoType(Types.ShortType.get()), SmallintType.SMALLINT);
    assertEquals(DataTypeTransformer.getTrinoType(Types.IntegerType.get()), IntegerType.INTEGER);
    assertEquals(DataTypeTransformer.getTrinoType(Types.LongType.get()), BigintType.BIGINT);

    assertEquals(DataTypeTransformer.getTrinoType(Types.FloatType.get()), RealType.REAL);
    assertEquals(DataTypeTransformer.getTrinoType(Types.DoubleType.get()), DoubleType.DOUBLE);
    assertEquals(
        DataTypeTransformer.getTrinoType(Types.DecimalType.of(10, 2)),
        DecimalType.createDecimalType(10, 2));

    assertEquals(
        DataTypeTransformer.getTrinoType(Types.FixedCharType.of(10)), CharType.createCharType(10));
    assertEquals(
        DataTypeTransformer.getTrinoType(Types.VarCharType.of(10)),
        VarcharType.createVarcharType(10));
    assertEquals(
        DataTypeTransformer.getTrinoType(Types.VarCharType.of(Integer.MAX_VALUE - 1)),
        VarcharType.createVarcharType(VarcharType.MAX_LENGTH));

    assertEquals(DataTypeTransformer.getTrinoType(Types.DateType.get()), DateType.DATE);
    assertEquals(DataTypeTransformer.getTrinoType(Types.TimeType.get()), TimeType.TIME_MILLIS);
    assertEquals(
        DataTypeTransformer.getTrinoType(Types.TimestampType.withoutTimeZone()),
        TimestampType.TIMESTAMP_MILLIS);

    assertEquals(
        DataTypeTransformer.getTrinoType(Types.ListType.nullable(Types.IntegerType.get())),
        new ArrayType(IntegerType.INTEGER));

    assertEquals(
        Types.MapType.of(Types.ShortType.get(), Types.IntegerType.get(), true),
        DataTypeTransformer.getGravitinoType(
            new MapType(SmallintType.SMALLINT, IntegerType.INTEGER, new TypeOperators())));

    assertEquals(
        DataTypeTransformer.getTrinoType(
            Types.StructType.of(
                Types.StructType.Field.nullableField("a", Types.IntegerType.get()),
                Types.StructType.Field.nullableField("b", Types.VarCharType.of(10)))),
        RowType.from(
            ImmutableList.of(
                new Field(Optional.of("a"), IntegerType.INTEGER),
                new Field(Optional.of("b"), VarcharType.createVarcharType(10)))));

    assertEquals(DataTypeTransformer.getTrinoType(Types.UUIDType.get()), UuidType.UUID);

    try {
      DataTypeTransformer.getTrinoType(Types.BinaryType.get());
    } catch (TrinoException e) {
      if (e.getErrorCode() != GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE.toErrorCode()) {
        throw e;
      }
    }
  }
}
