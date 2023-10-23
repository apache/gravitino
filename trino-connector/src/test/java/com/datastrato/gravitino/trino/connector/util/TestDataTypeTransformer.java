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

import com.datastrato.gravitino.shaded.io.substrait.type.TypeCreator;
import io.trino.spi.TrinoException;
import org.testng.annotations.Test;

public class TestDataTypeTransformer {

  @Test
  public void testGetGravitinoType() {
    assertEquals(DataTypeTransformer.getGravitinoType(VARCHAR, true), TypeCreator.NULLABLE.STRING);
    assertEquals(DataTypeTransformer.getGravitinoType(VARCHAR, false), TypeCreator.REQUIRED.STRING);

    assertEquals(DataTypeTransformer.getGravitinoType(BOOLEAN, true), TypeCreator.NULLABLE.BOOLEAN);
    assertEquals(
        DataTypeTransformer.getGravitinoType(BOOLEAN, false), TypeCreator.REQUIRED.BOOLEAN);

    assertEquals(DataTypeTransformer.getGravitinoType(INTEGER, true), TypeCreator.NULLABLE.I32);
    assertEquals(DataTypeTransformer.getGravitinoType(INTEGER, false), TypeCreator.REQUIRED.I32);

    assertEquals(DataTypeTransformer.getGravitinoType(BIGINT, true), TypeCreator.NULLABLE.I64);
    assertEquals(DataTypeTransformer.getGravitinoType(BIGINT, false), TypeCreator.REQUIRED.I64);

    assertEquals(DataTypeTransformer.getGravitinoType(DOUBLE, true), TypeCreator.NULLABLE.FP64);
    assertEquals(DataTypeTransformer.getGravitinoType(DOUBLE, false), TypeCreator.REQUIRED.FP64);

    assertEquals(
        DataTypeTransformer.getGravitinoType(VARBINARY, true), TypeCreator.NULLABLE.BINARY);
    assertEquals(
        DataTypeTransformer.getGravitinoType(VARBINARY, false), TypeCreator.REQUIRED.BINARY);

    assertEquals(DataTypeTransformer.getGravitinoType(DATE, true), TypeCreator.NULLABLE.DATE);
    assertEquals(DataTypeTransformer.getGravitinoType(DATE, false), TypeCreator.REQUIRED.DATE);

    assertEquals(
        DataTypeTransformer.getGravitinoType(TIMESTAMP_SECONDS, true),
        TypeCreator.NULLABLE.TIMESTAMP);
    assertEquals(
        DataTypeTransformer.getGravitinoType(TIMESTAMP_SECONDS, false),
        TypeCreator.REQUIRED.TIMESTAMP);

    try {
      DataTypeTransformer.getGravitinoType(HYPER_LOG_LOG, true);
    } catch (TrinoException e) {
      if (e.getErrorCode() != GRAVITINO_UNSUPPORTED_TRINO_DATATYPE.toErrorCode()) {
        throw e;
      }
    }
  }

  @Test
  public void testGetTrinoType() {
    assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.STRING), VARCHAR);
    assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.STRING), VARCHAR);

    assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.BOOLEAN), BOOLEAN);
    assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.BOOLEAN), BOOLEAN);

    assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.I32), INTEGER);
    assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.I32), INTEGER);

    assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.I64), BIGINT);
    assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.I64), BIGINT);

    assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.I64), BIGINT);
    assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.I64), BIGINT);

    assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.FP64), DOUBLE);
    assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.FP64), DOUBLE);

    assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.DATE), DATE);
    assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.DATE), DATE);

    assertEquals(
        DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.TIMESTAMP), TIMESTAMP_SECONDS);
    assertEquals(
        DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.TIMESTAMP), TIMESTAMP_SECONDS);

    try {
      DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.BINARY);
    } catch (TrinoException e) {
      if (e.getErrorCode() != GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE.toErrorCode()) {
        throw e;
      }
    }
  }
}
