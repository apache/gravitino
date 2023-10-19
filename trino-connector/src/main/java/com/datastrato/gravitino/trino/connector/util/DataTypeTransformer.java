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
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

import com.datastrato.gravitino.shaded.io.substrait.type.TypeCreator;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;

/** This class is used to transform datatype between gravitino and trino */
public class DataTypeTransformer {

  public static Type getTrinoType(com.datastrato.gravitino.shaded.io.substrait.type.Type type) {
    if (type.equals(TypeCreator.REQUIRED.STRING) || type.equals(TypeCreator.NULLABLE.STRING)) {
      return createUnboundedVarcharType();
    } else if (type.equals(TypeCreator.REQUIRED.BOOLEAN)
        || type.equals(TypeCreator.NULLABLE.BOOLEAN)) {
      return BOOLEAN;
    } else if (type.equals(TypeCreator.REQUIRED.I32) || type.equals(TypeCreator.NULLABLE.I32)) {
      return INTEGER;
    } else if (type.equals(TypeCreator.REQUIRED.I64) || type.equals(TypeCreator.NULLABLE.I64)) {
      return BIGINT;
    } else if (type.equals(TypeCreator.REQUIRED.FP64) || type.equals(TypeCreator.NULLABLE.FP64)) {
      return DOUBLE;
    } else if (type.equals(TypeCreator.REQUIRED.BINARY)
        || type.equals(TypeCreator.NULLABLE.BINARY)) {
      return VARBINARY;
    } else if (type.equals(TypeCreator.REQUIRED.DATE) || type.equals(TypeCreator.NULLABLE.DATE)) {
      return DATE;
    } else if (type.equals(TypeCreator.REQUIRED.TIMESTAMP)
        || type.equals(TypeCreator.NULLABLE.TIMESTAMP)) {
      return createTimestampType(TIMESTAMP_SECONDS.getPrecision());
    }
    throw new TrinoException(
        GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE, "Unsupported gravitino datatype: " + type);
  }

  public static com.datastrato.gravitino.shaded.io.substrait.type.Type getGravitinoType(
      Type type, boolean nullable) {
    if (type.equals(VARCHAR)) {
      return nullable ? TypeCreator.NULLABLE.STRING : TypeCreator.REQUIRED.STRING;
    } else if (type.equals(BOOLEAN)) {
      return nullable ? TypeCreator.NULLABLE.BOOLEAN : TypeCreator.REQUIRED.BOOLEAN;
    } else if (type.equals(INTEGER)) {
      return nullable ? TypeCreator.NULLABLE.I32 : TypeCreator.REQUIRED.I32;
    } else if (type.equals(BIGINT)) {
      return nullable ? TypeCreator.NULLABLE.I64 : TypeCreator.REQUIRED.I64;
    } else if (type.equals(DOUBLE)) {
      return nullable ? TypeCreator.NULLABLE.FP64 : TypeCreator.REQUIRED.FP64;
    } else if (type.equals(VARBINARY)) {
      return nullable ? TypeCreator.NULLABLE.BINARY : TypeCreator.REQUIRED.BINARY;
    } else if (type.equals(DATE)) {
      return nullable ? TypeCreator.NULLABLE.DATE : TypeCreator.REQUIRED.DATE;
    } else if (type.equals(TIMESTAMP_SECONDS)) {
      return nullable ? TypeCreator.NULLABLE.TIMESTAMP : TypeCreator.REQUIRED.TIMESTAMP;
    }
    throw new TrinoException(
        GRAVITINO_UNSUPPORTED_TRINO_DATATYPE, "Unsupported trino datatype: " + type);
  }
}
