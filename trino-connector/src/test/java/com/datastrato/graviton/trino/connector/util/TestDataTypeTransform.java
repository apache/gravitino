/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.util;

import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_UNSUPPORTED_GRAVITON_DATATYPE;
import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_UNSUPPORTED_TRINO_DATATYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.VarcharType.VARCHAR;

import io.substrait.type.TypeCreator;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDataTypeTransform {

  @Test
  public void testGetGravitonType() {
    Assertions.assertEquals(
        DataTypeTransform.getGravitonType(VARCHAR, true), TypeCreator.NULLABLE.STRING);
    Assertions.assertEquals(
        DataTypeTransform.getGravitonType(VARCHAR, false), TypeCreator.REQUIRED.STRING);

    Assertions.assertEquals(
        DataTypeTransform.getGravitonType(INTEGER, true), TypeCreator.NULLABLE.I32);
    Assertions.assertEquals(
        DataTypeTransform.getGravitonType(INTEGER, false), TypeCreator.REQUIRED.I32);

    Assertions.assertEquals(
        DataTypeTransform.getGravitonType(BIGINT, true), TypeCreator.NULLABLE.I64);
    Assertions.assertEquals(
        DataTypeTransform.getGravitonType(BIGINT, false), TypeCreator.REQUIRED.I64);

    Assertions.assertEquals(
        DataTypeTransform.getGravitonType(DATE, true), TypeCreator.NULLABLE.DATE);
    Assertions.assertEquals(
        DataTypeTransform.getGravitonType(DATE, false), TypeCreator.REQUIRED.DATE);

    Assertions.assertEquals(
        DataTypeTransform.getGravitonType(TIMESTAMP_SECONDS, true), TypeCreator.NULLABLE.TIMESTAMP);
    Assertions.assertEquals(
        DataTypeTransform.getGravitonType(TIMESTAMP_SECONDS, false),
        TypeCreator.REQUIRED.TIMESTAMP);

    try {
      DataTypeTransform.getGravitonType(HYPER_LOG_LOG, true);
    } catch (TrinoException e) {
      if (e.getErrorCode() != GRAVITON_UNSUPPORTED_TRINO_DATATYPE.toErrorCode()) {
        throw e;
      }
    }
  }

  @Test
  public void testGetTrinoType() {
    Assertions.assertEquals(DataTypeTransform.getTrinoType(TypeCreator.NULLABLE.STRING), VARCHAR);
    Assertions.assertEquals(DataTypeTransform.getTrinoType(TypeCreator.REQUIRED.STRING), VARCHAR);

    Assertions.assertEquals(DataTypeTransform.getTrinoType(TypeCreator.NULLABLE.I32), INTEGER);
    Assertions.assertEquals(DataTypeTransform.getTrinoType(TypeCreator.REQUIRED.I32), INTEGER);

    Assertions.assertEquals(DataTypeTransform.getTrinoType(TypeCreator.NULLABLE.I64), BIGINT);
    Assertions.assertEquals(DataTypeTransform.getTrinoType(TypeCreator.REQUIRED.I64), BIGINT);

    Assertions.assertEquals(DataTypeTransform.getTrinoType(TypeCreator.NULLABLE.DATE), DATE);
    Assertions.assertEquals(DataTypeTransform.getTrinoType(TypeCreator.REQUIRED.DATE), DATE);

    Assertions.assertEquals(
        DataTypeTransform.getTrinoType(TypeCreator.NULLABLE.TIMESTAMP), TIMESTAMP_SECONDS);
    Assertions.assertEquals(
        DataTypeTransform.getTrinoType(TypeCreator.REQUIRED.TIMESTAMP), TIMESTAMP_SECONDS);

    try {
      DataTypeTransform.getTrinoType(TypeCreator.NULLABLE.BINARY);
    } catch (TrinoException e) {
      if (e.getErrorCode() != GRAVITON_UNSUPPORTED_GRAVITON_DATATYPE.toErrorCode()) {
        throw e;
      }
    }
  }
}
