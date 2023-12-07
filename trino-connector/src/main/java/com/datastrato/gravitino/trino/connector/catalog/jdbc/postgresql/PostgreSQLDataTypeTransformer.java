/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc.postgresql;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Type.Name;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;

/** Type transformer between PostgreSQL and Trino */
public class PostgreSQLDataTypeTransformer extends GeneralDataTypeTransformer {

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    Type gravitinoType = super.getGravitinoType(type);
    if (gravitinoType.name() == Name.VARCHAR || gravitinoType.name() == Name.FIXEDCHAR) {
      return Types.StringType.get();
    }
    return gravitinoType;
  }
}
