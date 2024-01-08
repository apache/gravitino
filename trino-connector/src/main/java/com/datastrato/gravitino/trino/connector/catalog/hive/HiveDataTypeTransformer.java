/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Type.Name;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;

/** Type transformer between Hive and Trino */
public class HiveDataTypeTransformer extends GeneralDataTypeTransformer {
  // Max length of Hive varchar is 65535
  private static final int HIVE_VARCHAR_MAX_LENGTH = 65535;

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    Type gravitinoType = super.getGravitinoType(type);
    if (gravitinoType.name() == Name.VARCHAR
        && ((Types.VarCharType) gravitinoType).length() > HIVE_VARCHAR_MAX_LENGTH) {
      return Types.VarCharType.of(HIVE_VARCHAR_MAX_LENGTH);
    }

    if (gravitinoType.name() == Name.FIXEDCHAR
        && ((Types.FixedCharType) gravitinoType).length() > HIVE_VARCHAR_MAX_LENGTH) {
      return Types.FixedCharType.of(HIVE_VARCHAR_MAX_LENGTH);
    }

    return gravitinoType;
  }
}
