/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql;

import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Type.Name;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import java.util.Map;

/** Type transformer between MySQL and Trino */
public class MySQLDataTypeTransformer extends GeneralDataTypeTransformer {

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    for (Map.Entry<String, PropertyEntry<?>> entryEntry :
        HiveTablePropertiesMetadata.propertiesMetadata.entrySet()) {
      System.out.println(entryEntry.getKey() + " " + entryEntry.getValue());
    }

    Type gravitinoType = super.getGravitinoType(type);
    if (gravitinoType.name() == Name.VARCHAR || gravitinoType.name() == Name.FIXEDCHAR) {
      return Types.StringType.get();
    }
    return gravitinoType;
  }
}
