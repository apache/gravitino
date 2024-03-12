/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types.BinaryType;
import com.datastrato.gravitino.rel.types.Types.BooleanType;
import com.datastrato.gravitino.rel.types.Types.ByteType;
import com.datastrato.gravitino.rel.types.Types.DateType;
import com.datastrato.gravitino.rel.types.Types.DecimalType;
import com.datastrato.gravitino.rel.types.Types.DoubleType;
import com.datastrato.gravitino.rel.types.Types.FixedCharType;
import com.datastrato.gravitino.rel.types.Types.FloatType;
import com.datastrato.gravitino.rel.types.Types.IntegerType;
import com.datastrato.gravitino.rel.types.Types.LongType;
import com.datastrato.gravitino.rel.types.Types.NullType;
import com.datastrato.gravitino.rel.types.Types.ShortType;
import com.datastrato.gravitino.rel.types.Types.StringType;
import com.datastrato.gravitino.rel.types.Types.TimestampType;
import com.datastrato.gravitino.rel.types.Types.VarCharType;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Set;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestSparkTypeConverter {

  private HashMap<Type, DataType> gravitinoToSparkTypeMapper = new HashMap<>();
  private Set<Type> notSupportGravitinoTypes = ImmutableSet.of(NullType.get());

  private Set<DataType> notSupportSparkTypes = ImmutableSet.of();

  @BeforeAll
  void init() {
    gravitinoToSparkTypeMapper.put(ByteType.get(), DataTypes.ByteType);
    gravitinoToSparkTypeMapper.put(ShortType.get(), DataTypes.ShortType);
    gravitinoToSparkTypeMapper.put(IntegerType.get(), DataTypes.IntegerType);
    gravitinoToSparkTypeMapper.put(LongType.get(), DataTypes.LongType);
    gravitinoToSparkTypeMapper.put(FloatType.get(), DataTypes.FloatType);
    gravitinoToSparkTypeMapper.put(DoubleType.get(), DataTypes.DoubleType);
    gravitinoToSparkTypeMapper.put(DecimalType.of(10, 2), DataTypes.createDecimalType(10, 2));
    gravitinoToSparkTypeMapper.put(StringType.get(), DataTypes.StringType);
    gravitinoToSparkTypeMapper.put(VarCharType.of(2), VarcharType.apply(2));
    gravitinoToSparkTypeMapper.put(FixedCharType.of(2), CharType.apply(2));
    gravitinoToSparkTypeMapper.put(BinaryType.get(), DataTypes.BinaryType);
    gravitinoToSparkTypeMapper.put(BooleanType.get(), DataTypes.BooleanType);
    gravitinoToSparkTypeMapper.put(DateType.get(), DataTypes.DateType);
    gravitinoToSparkTypeMapper.put(TimestampType.withTimeZone(), DataTypes.TimestampType);
    gravitinoToSparkTypeMapper.put(TimestampType.withoutTimeZone(), DataTypes.TimestampNTZType);
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
