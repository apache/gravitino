/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector.utils;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

public class TypeUtils {

  private TypeUtils() {}

  public static Type toGravitinoType(LogicalType logicalType) {
    switch (logicalType.getTypeRoot()) {
      case VARCHAR:
        return Types.StringType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case INTEGER:
        return Types.IntegerType.get();
      default:
        throw new UnsupportedOperationException(
            "Not support type: " + logicalType.asSummaryString());
    }
  }

  public static DataType toFlinkType(Type gravitinoType) {
    if (gravitinoType instanceof Types.DoubleType) {
      return DataTypes.DOUBLE();
    } else if (gravitinoType instanceof Types.StringType) {
      return DataTypes.STRING();
    } else if (gravitinoType instanceof Types.IntegerType) {
      return DataTypes.INT();
    }
    throw new UnsupportedOperationException("Not support " + gravitinoType.toString());
  }
}
