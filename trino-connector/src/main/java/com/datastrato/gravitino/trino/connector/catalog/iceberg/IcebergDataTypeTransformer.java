/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Type.Name;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.trino.connector.GravitinoErrorCode;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import io.trino.spi.TrinoException;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

/** Type transformer between Iceberg and Trino */
public class IcebergDataTypeTransformer extends GeneralDataTypeTransformer {

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    Class<? extends io.trino.spi.type.Type> typeClass = type.getClass();
    if (typeClass == io.trino.spi.type.CharType.class) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          "Gravitino does not support the datatype CHAR");
    } else if (typeClass == io.trino.spi.type.VarcharType.class) {
      VarcharType varCharType = (VarcharType) type;
      if (varCharType.getLength().isPresent()) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "Gravitino does not support the datatype VARCHAR with length");
      }

      return Types.StringType.get();
    }

    return super.getGravitinoType(type);
  }

  @Override
  public io.trino.spi.type.Type getTrinoType(Type type) {
    if (Name.FIXED == type.name()) {
      return VarbinaryType.VARBINARY;
    }
    return super.getTrinoType(type);
  }
}
