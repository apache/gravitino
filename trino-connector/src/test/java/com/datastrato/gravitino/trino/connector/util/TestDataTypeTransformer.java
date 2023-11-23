/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.util;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_UNSUPPORTED_TRINO_DATATYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

import com.datastrato.gravitino.rel.types.Types;
import io.trino.spi.TrinoException;
import org.testng.annotations.Test;

public class TestDataTypeTransformer {

  @Test
  public void testGetGravitinoType() {
    assertEquals(DataTypeTransformer.getGravitinoType(VARCHAR), Types.StringType.get());
    assertEquals(DataTypeTransformer.getGravitinoType(VARCHAR), Types.StringType.get());

    assertEquals(DataTypeTransformer.getGravitinoType(BOOLEAN), Types.BooleanType.get());
    assertEquals(DataTypeTransformer.getGravitinoType(BOOLEAN), Types.BooleanType.get());

    assertEquals(DataTypeTransformer.getGravitinoType(INTEGER), Types.IntegerType.get());
    assertEquals(DataTypeTransformer.getGravitinoType(INTEGER), Types.IntegerType.get());

    assertEquals(DataTypeTransformer.getGravitinoType(BIGINT), Types.LongType.get());
    assertEquals(DataTypeTransformer.getGravitinoType(BIGINT), Types.LongType.get());

    assertEquals(DataTypeTransformer.getGravitinoType(DOUBLE), Types.DoubleType.get());
    assertEquals(DataTypeTransformer.getGravitinoType(DOUBLE), Types.DoubleType.get());

    assertEquals(DataTypeTransformer.getGravitinoType(VARBINARY), Types.BinaryType.get());
    assertEquals(DataTypeTransformer.getGravitinoType(VARBINARY), Types.BinaryType.get());

    assertEquals(DataTypeTransformer.getGravitinoType(DATE), Types.DateType.get());
    assertEquals(DataTypeTransformer.getGravitinoType(DATE), Types.DateType.get());

    assertEquals(
        DataTypeTransformer.getGravitinoType(TIMESTAMP_SECONDS),
        Types.TimestampType.withoutTimeZone());
    assertEquals(
        DataTypeTransformer.getGravitinoType(TIMESTAMP_SECONDS),
        Types.TimestampType.withoutTimeZone());

    try {
      DataTypeTransformer.getGravitinoType(HYPER_LOG_LOG);
    } catch (TrinoException e) {
      if (e.getErrorCode() != GRAVITINO_UNSUPPORTED_TRINO_DATATYPE.toErrorCode()) {
        throw e;
      }
    }
  }

  @Test
  public void testGetTrinoType() {
    assertEquals(DataTypeTransformer.getTrinoType(Types.StringType.get()), VARCHAR);
    assertEquals(DataTypeTransformer.getTrinoType(Types.StringType.get()), VARCHAR);

    assertEquals(DataTypeTransformer.getTrinoType(Types.BooleanType.get()), BOOLEAN);
    assertEquals(DataTypeTransformer.getTrinoType(Types.BooleanType.get()), BOOLEAN);

    assertEquals(DataTypeTransformer.getTrinoType(Types.IntegerType.get()), INTEGER);
    assertEquals(DataTypeTransformer.getTrinoType(Types.IntegerType.get()), INTEGER);

    assertEquals(DataTypeTransformer.getTrinoType(Types.LongType.get()), BIGINT);
    assertEquals(DataTypeTransformer.getTrinoType(Types.LongType.get()), BIGINT);

    assertEquals(DataTypeTransformer.getTrinoType(Types.LongType.get()), BIGINT);
    assertEquals(DataTypeTransformer.getTrinoType(Types.LongType.get()), BIGINT);

    assertEquals(DataTypeTransformer.getTrinoType(Types.DoubleType.get()), DOUBLE);
    assertEquals(DataTypeTransformer.getTrinoType(Types.DoubleType.get()), DOUBLE);

    assertEquals(DataTypeTransformer.getTrinoType(Types.DateType.get()), DATE);
    assertEquals(DataTypeTransformer.getTrinoType(Types.DateType.get()), DATE);

    assertEquals(
        DataTypeTransformer.getTrinoType(Types.TimestampType.withoutTimeZone()), TIMESTAMP_SECONDS);
    assertEquals(
        DataTypeTransformer.getTrinoType(Types.TimestampType.withoutTimeZone()), TIMESTAMP_SECONDS);

    try {
      DataTypeTransformer.getTrinoType(Types.BinaryType.get());
    } catch (TrinoException e) {
      if (e.getErrorCode() != GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE.toErrorCode()) {
        throw e;
      }
    }
  }
}
