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

import io.substrait.type.TypeCreator;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDataTypeTransformer {

  @Test
  public void testGetGravitinoType() {
    Assertions.assertEquals(
        DataTypeTransformer.getGravitinoType(VARCHAR, true), TypeCreator.NULLABLE.STRING);
    Assertions.assertEquals(
        DataTypeTransformer.getGravitinoType(VARCHAR, false), TypeCreator.REQUIRED.STRING);

    Assertions.assertEquals(
        DataTypeTransformer.getGravitinoType(BOOLEAN, true), TypeCreator.NULLABLE.BOOLEAN);
    Assertions.assertEquals(
        DataTypeTransformer.getGravitinoType(BOOLEAN, false), TypeCreator.REQUIRED.BOOLEAN);

    Assertions.assertEquals(
        DataTypeTransformer.getGravitinoType(INTEGER, true), TypeCreator.NULLABLE.I32);
    Assertions.assertEquals(
        DataTypeTransformer.getGravitinoType(INTEGER, false), TypeCreator.REQUIRED.I32);

    Assertions.assertEquals(
        DataTypeTransformer.getGravitinoType(BIGINT, true), TypeCreator.NULLABLE.I64);
    Assertions.assertEquals(
        DataTypeTransformer.getGravitinoType(BIGINT, false), TypeCreator.REQUIRED.I64);

    Assertions.assertEquals(
        DataTypeTransformer.getGravitinoType(DOUBLE, true), TypeCreator.NULLABLE.FP64);
    Assertions.assertEquals(
        DataTypeTransformer.getGravitinoType(DOUBLE, false), TypeCreator.REQUIRED.FP64);

    Assertions.assertEquals(
        DataTypeTransformer.getGravitinoType(VARBINARY, true), TypeCreator.NULLABLE.BINARY);
    Assertions.assertEquals(
        DataTypeTransformer.getGravitinoType(VARBINARY, false), TypeCreator.REQUIRED.BINARY);

    Assertions.assertEquals(
        DataTypeTransformer.getGravitinoType(DATE, true), TypeCreator.NULLABLE.DATE);
    Assertions.assertEquals(
        DataTypeTransformer.getGravitinoType(DATE, false), TypeCreator.REQUIRED.DATE);

    Assertions.assertEquals(
        DataTypeTransformer.getGravitinoType(TIMESTAMP_SECONDS, true),
        TypeCreator.NULLABLE.TIMESTAMP);
    Assertions.assertEquals(
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
    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.STRING), VARCHAR);
    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.STRING), VARCHAR);

    Assertions.assertEquals(
        DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.BOOLEAN), BOOLEAN);
    Assertions.assertEquals(
        DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.BOOLEAN), BOOLEAN);

    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.I32), INTEGER);
    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.I32), INTEGER);

    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.I64), BIGINT);
    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.I64), BIGINT);

    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.I64), BIGINT);
    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.I64), BIGINT);

    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.FP64), DOUBLE);
    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.FP64), DOUBLE);

    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.DATE), DATE);
    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.DATE), DATE);

    Assertions.assertEquals(
        DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.TIMESTAMP), TIMESTAMP_SECONDS);
    Assertions.assertEquals(
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
