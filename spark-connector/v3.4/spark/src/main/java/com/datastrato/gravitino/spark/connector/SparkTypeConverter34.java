/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.TimestampNTZType;

public class SparkTypeConverter34 extends SparkTypeConverter {

  public Type toGravitinoType(DataType sparkType) {
    if (sparkType instanceof TimestampNTZType) {
      return Types.TimestampType.withoutTimeZone();
    } else {
      return super.toGravitinoType(sparkType);
    }
  }

  public DataType toSparkType(Type gravitinoType) {
    if (gravitinoType instanceof Types.TimestampType
        && ((Types.TimestampType) gravitinoType).hasTimeZone() == false) {
      return DataTypes.TimestampNTZType;
    } else {
      return super.toSparkType(gravitinoType);
    }
  }
}
