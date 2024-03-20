/*
 * Copyright 2023 Datastrato Pvt Ltd.
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

  GeneralDataTypeTransformer dataTypeTransformer = new GeneralDataTypeTransformer();

  @Test
  public void testGetGravitinoType() {
    assertEquals(
        dataTypeTransformer.getGravitinoType(BooleanType.BOOLEAN), Types.BooleanType.get());

    assertEquals(dataTypeTransformer.getGravitinoType(TinyintType.TINYINT), Types.ByteType.get());
    assertEquals(
        dataTypeTransformer.getGravitinoType(SmallintType.SMALLINT), Types.ShortType.get());
    assertEquals(
        dataTypeTransformer.getGravitinoType(IntegerType.INTEGER), Types.IntegerType.get());
    assertEquals(dataTypeTransformer.getGravitinoType(BigintType.BIGINT), Types.LongType.get());

    assertEquals(dataTypeTransformer.getGravitinoType(RealType.REAL), Types.FloatType.get());
    assertEquals(dataTypeTransformer.getGravitinoType(DoubleType.DOUBLE), Types.DoubleType.get());
    assertEquals(
        dataTypeTransformer.getGravitinoType(DecimalType.createDecimalType(10, 2)),
        Types.DecimalType.of(10, 2));

    assertEquals(
        dataTypeTransformer.getGravitinoType(CharType.createCharType(10)),
        Types.FixedCharType.of(10));
    assertEquals(
        dataTypeTransformer.getGravitinoType(VarcharType.createVarcharType(10)),
        Types.VarCharType.of(10));
    assertEquals(
        dataTypeTransformer.getGravitinoType(VarcharType.createVarcharType(VarcharType.MAX_LENGTH)),
        Types.VarCharType.of(Integer.MAX_VALUE - 1));

    assertEquals(dataTypeTransformer.getGravitinoType(DateType.DATE), Types.DateType.get());
    assertEquals(dataTypeTransformer.getGravitinoType(TimeType.TIME_MILLIS), Types.TimeType.get());
    assertEquals(
        dataTypeTransformer.getGravitinoType(TimestampType.TIMESTAMP_MILLIS),
        Types.TimestampType.withoutTimeZone());

    assertEquals(
        dataTypeTransformer.getGravitinoType(new ArrayType(IntegerType.INTEGER)),
        Types.ListType.nullable(Types.IntegerType.get()));
    assertEquals(
        dataTypeTransformer.getGravitinoType(
            new MapType(SmallintType.SMALLINT, IntegerType.INTEGER, new TypeOperators())),
        Types.MapType.of(Types.ShortType.get(), Types.IntegerType.get(), true));

    assertEquals(
        dataTypeTransformer.getGravitinoType(
            RowType.from(
                ImmutableList.of(
                    new Field(Optional.of("a"), IntegerType.INTEGER),
                    new Field(Optional.of("b"), VarcharType.createVarcharType(10))))),
        Types.StructType.of(
            Types.StructType.Field.nullableField("a", Types.IntegerType.get()),
            Types.StructType.Field.nullableField("b", Types.VarCharType.of(10))));

    assertEquals(dataTypeTransformer.getGravitinoType(UuidType.UUID), Types.UUIDType.get());

    try {
      dataTypeTransformer.getGravitinoType(HyperLogLogType.HYPER_LOG_LOG);
    } catch (TrinoException e) {
      if (!GRAVITINO_UNSUPPORTED_TRINO_DATATYPE.toErrorCode().equals(e.getErrorCode())) {
        throw e;
      }
    }
  }

  @Test
  public void testGetTrinoType() {
    assertEquals(dataTypeTransformer.getTrinoType(Types.BooleanType.get()), BooleanType.BOOLEAN);

    assertEquals(dataTypeTransformer.getTrinoType(Types.ByteType.get()), TinyintType.TINYINT);
    assertEquals(dataTypeTransformer.getTrinoType(Types.ShortType.get()), SmallintType.SMALLINT);
    assertEquals(dataTypeTransformer.getTrinoType(Types.IntegerType.get()), IntegerType.INTEGER);
    assertEquals(dataTypeTransformer.getTrinoType(Types.LongType.get()), BigintType.BIGINT);

    assertEquals(dataTypeTransformer.getTrinoType(Types.FloatType.get()), RealType.REAL);
    assertEquals(dataTypeTransformer.getTrinoType(Types.DoubleType.get()), DoubleType.DOUBLE);
    assertEquals(
        dataTypeTransformer.getTrinoType(Types.DecimalType.of(10, 2)),
        DecimalType.createDecimalType(10, 2));

    assertEquals(
        dataTypeTransformer.getTrinoType(Types.FixedCharType.of(10)), CharType.createCharType(10));
    assertEquals(
        dataTypeTransformer.getTrinoType(Types.VarCharType.of(10)),
        VarcharType.createVarcharType(10));
    assertEquals(
        dataTypeTransformer.getTrinoType(Types.VarCharType.of(Integer.MAX_VALUE - 1)),
        VarcharType.createVarcharType(VarcharType.MAX_LENGTH));

    assertEquals(dataTypeTransformer.getTrinoType(Types.DateType.get()), DateType.DATE);
    assertEquals(dataTypeTransformer.getTrinoType(Types.TimeType.get()), TimeType.TIME_MILLIS);
    assertEquals(
        dataTypeTransformer.getTrinoType(Types.TimestampType.withoutTimeZone()),
        TimestampType.TIMESTAMP_MILLIS);

    assertEquals(
        dataTypeTransformer.getTrinoType(Types.ListType.nullable(Types.IntegerType.get())),
        new ArrayType(IntegerType.INTEGER));

    assertEquals(
        Types.MapType.of(Types.ShortType.get(), Types.IntegerType.get(), true),
        dataTypeTransformer.getGravitinoType(
            new MapType(SmallintType.SMALLINT, IntegerType.INTEGER, new TypeOperators())));

    assertEquals(
        dataTypeTransformer.getTrinoType(
            Types.StructType.of(
                Types.StructType.Field.nullableField("a", Types.IntegerType.get()),
                Types.StructType.Field.nullableField("b", Types.VarCharType.of(10)))),
        RowType.from(
            ImmutableList.of(
                new Field(Optional.of("a"), IntegerType.INTEGER),
                new Field(Optional.of("b"), VarcharType.createVarcharType(10)))));

    assertEquals(dataTypeTransformer.getTrinoType(Types.UUIDType.get()), UuidType.UUID);

    try {
      dataTypeTransformer.getTrinoType(Types.BinaryType.get());
    } catch (TrinoException e) {
      if (!GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE.toErrorCode().equals(e.getErrorCode())) {
        throw e;
      }
    }
  }
}
