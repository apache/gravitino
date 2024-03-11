/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StringType;

/** Transform DataTypes between Gravitino and Spark. */
public class SparkTypeConverter {
  public static Type toGravitinoType(DataType sparkType) {
    if (sparkType instanceof StringType) {
      return Types.StringType.get();
    } else if (sparkType instanceof BooleanType) {
      return Types.BooleanType.get();
    } else if (sparkType instanceof IntegerType) {
      return Types.IntegerType.get();
    } else if (sparkType instanceof FloatType) {
      return Types.FloatType.get();
    } else if (sparkType instanceof DoubleType) {
      return Types.DoubleType.get();
    }
    throw new UnsupportedOperationException("Not support " + sparkType.toString());
  }

  public static DataType toSparkType(Type gravitinoType) {
    if (gravitinoType instanceof Types.StringType) {
      return DataTypes.StringType;
    } else if (gravitinoType instanceof Types.BooleanType) {
      return DataTypes.BooleanType;
    } else if (gravitinoType instanceof Types.IntegerType) {
      return DataTypes.IntegerType;
    } else if (gravitinoType instanceof Types.FloatType) {
      return DataTypes.FloatType;
    } else if (gravitinoType instanceof Types.DoubleType) {
      return DataTypes.DoubleType;
    }
    throw new UnsupportedOperationException("Not support " + gravitinoType.toString());
  }
}
