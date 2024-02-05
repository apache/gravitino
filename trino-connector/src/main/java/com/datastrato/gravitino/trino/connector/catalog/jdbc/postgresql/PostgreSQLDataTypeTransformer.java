/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc.postgresql;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;

/** Type transformer between PostgreSQL and Trino */
public class PostgreSQLDataTypeTransformer extends GeneralDataTypeTransformer {

  private static final int MAX_VARCHAR_LENGTH_FOR_PG = 10485760;

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    if (type.equals(io.trino.spi.type.VarcharType.VARCHAR)) {
      return Types.VarCharType.of(MAX_VARCHAR_LENGTH_FOR_PG);
    }

    return super.getGravitinoType(type);
  }
}
