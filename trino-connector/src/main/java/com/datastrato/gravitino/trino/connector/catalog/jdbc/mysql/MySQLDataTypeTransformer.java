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
import io.trino.spi.type.CharType;

/** Type transformer between MySQL and Trino */
public class MySQLDataTypeTransformer extends GeneralDataTypeTransformer {
  private static final int MYSQL_CHAR_LENGTH_LIMIT = 255;
  // 65535 / 4 = 16383, in fact, MySQL limit the row size to 65535, and the utf8mb4 character set
  // uses 4 bytes per character. In fact, if a row has several varchar columns, the length of each
  // column should be less than 16383. For more details, please refer to
  // https://dev.mysql.com/doc/refman/8.0/en/char.html
  private static final int MYSQL_VARCHAR_LENGTH_LIMIT = 16383;

  @Override
  public io.trino.spi.type.Type getTrinoType(Type type) {
    if (type.name() == Name.STRING) {
      return io.trino.spi.type.VarcharType.createUnboundedVarcharType();
    }

    return super.getTrinoType(type);
  }

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    Class<? extends io.trino.spi.type.Type> typeClass = type.getClass();
    if (typeClass == io.trino.spi.type.CharType.class) {
      CharType charType = (CharType) type;
      if (charType.getLength() > MYSQL_CHAR_LENGTH_LIMIT) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "MySQL does not support the datatype CHAR with the length greater than "
                + MYSQL_CHAR_LENGTH_LIMIT);
      }

      // We do not support the CHAR without a length.
      if (charType.getLength() == 0) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "MySQL does not support the datatype CHAR with the length 0");
      }

      return Types.FixedCharType.of(charType.getLength());
    } else if (typeClass == io.trino.spi.type.VarcharType.class) {
      io.trino.spi.type.VarcharType varcharType = (io.trino.spi.type.VarcharType) type;

      // If the length is not specified, it is a VARCHAR without length, we convert it to a string
      // type.
      if (varcharType.getLength().isEmpty()) {
        return Types.StringType.get();
      }

      int length = varcharType.getLength().get();
      if (length > MYSQL_VARCHAR_LENGTH_LIMIT) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "MySQL does not support the datatype VARCHAR with the length greater than "
                + MYSQL_VARCHAR_LENGTH_LIMIT);
      }
      return Types.VarCharType.of(length);
    }

    return super.getGravitinoType(type);
  }
}
