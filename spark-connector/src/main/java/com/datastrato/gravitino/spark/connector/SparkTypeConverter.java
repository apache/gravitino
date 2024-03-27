/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.ArrayType;
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
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
    } else if (sparkType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) sparkType;
      return Types.ListType.of(toGravitinoType(arrayType.elementType()), arrayType.containsNull());
    } else if (sparkType instanceof MapType) {
      MapType mapType = (MapType) sparkType;
      return Types.MapType.of(
          toGravitinoType(mapType.keyType()),
          toGravitinoType(mapType.valueType()),
          mapType.valueContainsNull());
    } else if (sparkType instanceof StructType) {
      StructType structType = (StructType) sparkType;
      Types.StructType.Field[] fields =
          Arrays.stream(structType.fields())
              .map(
                  f ->
                      Types.StructType.Field.of(
                          f.name(),
                          toGravitinoType(f.dataType()),
                          f.nullable(),
                          f.getComment().isDefined() ? f.getComment().get() : null))
              .toArray(Types.StructType.Field[]::new);
      return Types.StructType.of(fields);
    } else if (sparkType instanceof NullType) {
      return Types.NullType.get();
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
    } else if (gravitinoType instanceof Types.ListType) {
      Types.ListType listType = (Types.ListType) gravitinoType;
      return DataTypes.createArrayType(
          toSparkType(listType.elementType()), listType.elementNullable());
    } else if (gravitinoType instanceof Types.MapType) {
      Types.MapType mapType = (Types.MapType) gravitinoType;
      return DataTypes.createMapType(
          toSparkType(mapType.keyType()),
          toSparkType(mapType.valueType()),
          mapType.valueNullable());
    } else if (gravitinoType instanceof Types.StructType) {
      Types.StructType structType = (Types.StructType) gravitinoType;
      List<StructField> fields =
          Arrays.stream(structType.fields())
              .map(
                  f ->
                      DataTypes.createStructField(
                          f.name(),
                          toSparkType(f.type()),
                          f.nullable(),
                          f.comment() == null
                              ? new MetadataBuilder().build()
                              : new MetadataBuilder()
                                  .putString(ConnectorConstants.COMMENT, f.comment())
                                  .build()))
              .collect(Collectors.toList());
      return DataTypes.createStructType(fields);
    } else if (gravitinoType instanceof Types.NullType) {
      return DataTypes.NullType;
    }
    throw new UnsupportedOperationException("Not support " + gravitinoType.toString());
  }
}
