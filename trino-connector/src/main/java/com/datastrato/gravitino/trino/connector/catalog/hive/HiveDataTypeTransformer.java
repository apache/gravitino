/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.trino.connector.GravitinoErrorCode;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import io.trino.spi.TrinoException;
import io.trino.spi.type.VarcharType;

/** Type transformer between Hive and Trino */
public class HiveDataTypeTransformer extends GeneralDataTypeTransformer {
  // Max length of Hive varchar is 65535
  private static final int HIVE_VARCHAR_MAX_LENGTH = 65535;
  private static final int HIVE_CHAR_MAX_LENGTH = 255;

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    Class<? extends io.trino.spi.type.Type> typeClass = type.getClass();
    if (typeClass == VarcharType.class) {
      VarcharType varcharType = (VarcharType) type;
      if (varcharType.getLength().isEmpty()) {
        return Types.StringType.get();
      }

      int length = varcharType.getLength().get();
      if (length > HIVE_VARCHAR_MAX_LENGTH) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "Hive does not support the datatype VARCHAR with the length greater than "
                + HIVE_VARCHAR_MAX_LENGTH
                + ", you can use varchar without length instead");
      }

      return Types.VarCharType.of(length);
    } else if (typeClass == io.trino.spi.type.CharType.class) {
      io.trino.spi.type.CharType charType = (io.trino.spi.type.CharType) type;
      if (charType.getLength() > HIVE_CHAR_MAX_LENGTH) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "Hive does not support the datatype CHAR with the length greater than "
                + HIVE_CHAR_MAX_LENGTH);
      }

      return Types.FixedCharType.of(charType.getLength());
    }

    return super.getGravitinoType(type);
  }
}
