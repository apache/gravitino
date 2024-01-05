/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Type.Name;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;

/** Type transformer between MySQL and Trino */
public class MySQLDataTypeTransformer extends GeneralDataTypeTransformer {

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    Type gravitinoType = super.getGravitinoType(type);
    if (gravitinoType.name() == Name.VARCHAR
        && ((Types.VarCharType) gravitinoType).length() > 16383) {
      return Types.VarCharType.of(16383);
    }

    if (gravitinoType.name() == Name.FIXEDCHAR
        && ((Types.FixedCharType) gravitinoType).length() > 16383) {
      return Types.FixedCharType.of(16383);
    }
    return gravitinoType;
  }
}
