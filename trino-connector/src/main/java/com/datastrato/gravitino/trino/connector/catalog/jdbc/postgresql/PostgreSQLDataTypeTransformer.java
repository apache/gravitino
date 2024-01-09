/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc.postgresql;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;

/** Type transformer between PostgreSQL and Trino */
public class PostgreSQLDataTypeTransformer extends GeneralDataTypeTransformer {

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    return super.getGravitinoType(type);
  }
}
