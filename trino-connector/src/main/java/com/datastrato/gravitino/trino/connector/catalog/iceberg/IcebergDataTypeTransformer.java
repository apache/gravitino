/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Type.Name;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import io.trino.spi.type.VarbinaryType;

/** Type transformer between Iceberg and Trino */
public class IcebergDataTypeTransformer extends GeneralDataTypeTransformer {

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    Type gravitinoType = super.getGravitinoType(type);
    if (gravitinoType.name() == Name.VARCHAR || gravitinoType.name() == Name.FIXEDCHAR) {
      return Types.StringType.get();
    }
    return gravitinoType;
  }

  @Override
  public io.trino.spi.type.Type getTrinoType(Type type) {
    if (Name.FIXED == type.name()) {
      return VarbinaryType.VARBINARY;
    }
    return super.getTrinoType(type);
  }
}
