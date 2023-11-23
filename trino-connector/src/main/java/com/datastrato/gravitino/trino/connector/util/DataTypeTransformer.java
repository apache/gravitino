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
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;

import com.datastrato.gravitino.rel.types.Types;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

/** This class is used to transform datatype between gravitino and trino */
public class DataTypeTransformer {

  public static Type getTrinoType(com.datastrato.gravitino.rel.types.Type type) {
    switch (type.name()) {
      case BOOLEAN:
        return BOOLEAN;
      case INTEGER:
        return INTEGER;
      case LONG:
        return BIGINT;
      case DOUBLE:
        return DOUBLE;
      case BINARY:
        return VARBINARY;
      case DATE:
        return DATE;
      case TIMESTAMP:
        return TIMESTAMP_SECONDS;
      case STRING:
        return VARCHAR;
      default:
        throw new TrinoException(
            GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE, "Unsupported gravitino datatype: " + type);
    }
  }

  public static com.datastrato.gravitino.rel.types.Type getGravitinoType(Type type) {
    if (type.equals(VARCHAR)) {
      return ((VarcharType) type).getLength().isPresent()
          ? Types.VarCharType.of(((VarcharType) type).getLength().get())
          : Types.StringType.get();
    } else if (type.equals(BOOLEAN)) {
      return Types.BooleanType.get();
    } else if (type.equals(INTEGER)) {
      return Types.IntegerType.get();
    } else if (type.equals(BIGINT)) {
      return Types.LongType.get();
    } else if (type.equals(DOUBLE)) {
      return Types.DoubleType.get();
    } else if (type.equals(VARBINARY)) {
      return Types.BinaryType.get();
    } else if (type.equals(DATE)) {
      return Types.DateType.get();
    } else if (type.equals(TIMESTAMP_SECONDS)) {
      return Types.TimestampType.withoutTimeZone();
    }
    throw new TrinoException(
        GRAVITINO_UNSUPPORTED_TRINO_DATATYPE, "Unsupported Trino datatype: " + type);
  }
}
