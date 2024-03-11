/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.rel.types.Types.BooleanType;
import com.datastrato.gravitino.rel.types.Types.DoubleType;
import com.datastrato.gravitino.rel.types.Types.FloatType;
import com.datastrato.gravitino.rel.types.Types.IntegerType;
import com.datastrato.gravitino.rel.types.Types.StringType;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Set;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestSparkTypeConverter {

  private HashMap<Type, DataType> gravitinoToSparkTypeMapper = new HashMap<>();
  private Set<Type> notSupportGravitinoTypes = ImmutableSet.of();

  private Set<DataType> notSupportSparkTypes = ImmutableSet.of();

  @BeforeAll
  void init() {
    gravitinoToSparkTypeMapper.put(IntegerType.get(), DataTypes.IntegerType);
    gravitinoToSparkTypeMapper.put(BooleanType.get(), DataTypes.BooleanType);
    gravitinoToSparkTypeMapper.put(StringType.get(), DataTypes.StringType);
    gravitinoToSparkTypeMapper.put(FloatType.get(), DataTypes.FloatType);
    gravitinoToSparkTypeMapper.put(DoubleType.get(), DataTypes.DoubleType);
    gravitinoToSparkTypeMapper.put(Types.ByteType.get(), DataTypes.ByteType);
    gravitinoToSparkTypeMapper.put(Types.ShortType.get(), DataTypes.ShortType);
    gravitinoToSparkTypeMapper.put(Types.LongType.get(), DataTypes.LongType);
    gravitinoToSparkTypeMapper.put(Types.DecimalType.of(10, 2), DecimalType.apply(10, 2));
    gravitinoToSparkTypeMapper.put(Types.BinaryType.get(), DataTypes.BinaryType);
    gravitinoToSparkTypeMapper.put(Types.FixedCharType.of(2), CharType.apply(2));
    gravitinoToSparkTypeMapper.put(Types.VarCharType.of(2), VarcharType.apply(2));
    gravitinoToSparkTypeMapper.put(Types.DateType.get(), DataTypes.DateType);
    gravitinoToSparkTypeMapper.put(Types.TimestampType.withTimeZone(), DataTypes.TimestampType);
    gravitinoToSparkTypeMapper.put(
        Types.TimestampType.withoutTimeZone(), DataTypes.TimestampNTZType);
    gravitinoToSparkTypeMapper.put(Types.NullType.get(), DataTypes.NullType);
  }

  @Test
  void testConvertGravitinoTypeToSpark() {
    gravitinoToSparkTypeMapper.forEach(
        (gravitinoType, sparkType) ->
            Assertions.assertEquals(sparkType, SparkTypeConverter.toSparkType(gravitinoType)));

    notSupportGravitinoTypes.forEach(
        gravitinoType ->
            Assertions.assertThrowsExactly(
                UnsupportedOperationException.class,
                () -> SparkTypeConverter.toSparkType(gravitinoType)));
  }

  @Test
  void testConvertSparkTypeToGravitino() {
    gravitinoToSparkTypeMapper.forEach(
        (gravitinoType, sparkType) ->
            Assertions.assertEquals(gravitinoType, SparkTypeConverter.toGravitinoType(sparkType)));

    notSupportSparkTypes.forEach(
        sparkType ->
            Assertions.assertThrowsExactly(
                UnsupportedOperationException.class,
                () -> SparkTypeConverter.toGravitinoType(sparkType)));
  }
}
