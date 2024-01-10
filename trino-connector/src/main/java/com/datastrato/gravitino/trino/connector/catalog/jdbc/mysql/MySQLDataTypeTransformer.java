/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Type.Name;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.trino.connector.GravitinoErrorCode;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import io.trino.spi.TrinoException;

/** Type transformer between MySQL and Trino */
public class MySQLDataTypeTransformer extends GeneralDataTypeTransformer {
  private static final int MYSQL_CHAR_LENGTH_LIMIT = 255;

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    Type gravitinoType = super.getGravitinoType(type);
    if (gravitinoType.name() == Name.VARCHAR) {
      if (((Types.VarCharType) gravitinoType).length() <= MYSQL_CHAR_LENGTH_LIMIT) {
        return Types.VarCharType.of(((Types.VarCharType) gravitinoType).length());
      } else {
        return Types.StringType.get();
      }
    }

    if (gravitinoType.name() == Name.FIXEDCHAR) {
      if (((Types.FixedCharType) gravitinoType).length() <= MYSQL_CHAR_LENGTH_LIMIT) {
        return Types.FixedCharType.of(((Types.FixedCharType) gravitinoType).length());
      } else {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "MySQL does not support fixed char length > 255");
      }
    }
    return gravitinoType;
  }
}
