/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;

/** Type transformer between MySQL and Trino */
public class MySQLDataTypeTransformer extends GeneralDataTypeTransformer {

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    return super.getGravitinoType(type);
  }
}
