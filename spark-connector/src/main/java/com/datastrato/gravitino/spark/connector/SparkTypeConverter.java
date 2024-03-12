/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.VarcharType;

/** Transform DataTypes between Gravitino and Spark. */
public class SparkTypeConverter {
  public static Type toGravitinoType(DataType sparkType) {
    if (sparkType instanceof ByteType) {
      return Types.ByteType.get();
    } else if (sparkType instanceof ShortType) {
      return Types.ShortType.get();
    } else if (sparkType instanceof IntegerType) {
      return Types.IntegerType.get();
    } else if (sparkType instanceof LongType) {
      return Types.LongType.get();
    } else if (sparkType instanceof FloatType) {
      return Types.FloatType.get();
    } else if (sparkType instanceof DoubleType) {
      return Types.DoubleType.get();
    } else if (sparkType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) sparkType;
      return Types.DecimalType.of(decimalType.precision(), decimalType.scale());
    } else if (sparkType instanceof StringType) {
      return Types.StringType.get();
    } else if (sparkType instanceof VarcharType) {
      VarcharType varcharType = (VarcharType) sparkType;
      return Types.VarCharType.of(varcharType.length());
    } else if (sparkType instanceof CharType) {
      CharType charType = (CharType) sparkType;
      return Types.FixedCharType.of(charType.length());
    } else if (sparkType instanceof BinaryType) {
      return Types.BinaryType.get();
    } else if (sparkType instanceof BooleanType) {
      return Types.BooleanType.get();
    } else if (sparkType instanceof DateType) {
      return Types.DateType.get();
    } else if (sparkType instanceof TimestampType) {
      return Types.TimestampType.withTimeZone();
    } else if (sparkType instanceof TimestampNTZType) {
      return Types.TimestampType.withoutTimeZone();
    }
    throw new UnsupportedOperationException("Not support " + sparkType.toString());
  }

  public static DataType toSparkType(Type gravitinoType) {
    if (gravitinoType instanceof Types.ByteType) {
      return DataTypes.ByteType;
    } else if (gravitinoType instanceof Types.ShortType) {
      return DataTypes.ShortType;
    } else if (gravitinoType instanceof Types.IntegerType) {
      return DataTypes.IntegerType;
    } else if (gravitinoType instanceof Types.LongType) {
      return DataTypes.LongType;
    } else if (gravitinoType instanceof Types.FloatType) {
      return DataTypes.FloatType;
    } else if (gravitinoType instanceof Types.DoubleType) {
      return DataTypes.DoubleType;
    } else if (gravitinoType instanceof Types.DecimalType) {
      Types.DecimalType decimalType = (Types.DecimalType) gravitinoType;
      return DataTypes.createDecimalType(decimalType.precision(), decimalType.scale());
    } else if (gravitinoType instanceof Types.StringType) {
      return DataTypes.StringType;
    } else if (gravitinoType instanceof Types.VarCharType) {
      Types.VarCharType varCharType = (Types.VarCharType) gravitinoType;
      return VarcharType.apply(varCharType.length());
    } else if (gravitinoType instanceof Types.FixedCharType) {
      Types.FixedCharType charType = (Types.FixedCharType) gravitinoType;
      return CharType.apply((charType.length()));
    } else if (gravitinoType instanceof Types.BinaryType) {
      return DataTypes.BinaryType;
    } else if (gravitinoType instanceof Types.BooleanType) {
      return DataTypes.BooleanType;
    } else if (gravitinoType instanceof Types.DateType) {
      return DataTypes.DateType;
    } else if (gravitinoType instanceof Types.TimestampType
        && ((Types.TimestampType) gravitinoType).hasTimeZone()) {
      return DataTypes.TimestampType;
    } else if (gravitinoType instanceof Types.TimestampType
        && !((Types.TimestampType) gravitinoType).hasTimeZone()) {
      return DataTypes.TimestampNTZType;
    }
    throw new UnsupportedOperationException("Not support " + gravitinoType.toString());
  }
}
