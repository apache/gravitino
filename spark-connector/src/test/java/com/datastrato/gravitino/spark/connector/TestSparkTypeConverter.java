/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types.BooleanType;
import com.datastrato.gravitino.rel.types.Types.IntegerType;
import com.datastrato.gravitino.rel.types.Types.NullType;
import com.datastrato.gravitino.rel.types.Types.StringType;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Set;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestSparkTypeConverter {

  private HashMap<Type, DataType> gravitinoToSparkTypeMapper = new HashMap<>();
  private Set<Type> notSupportGravitinoTypes = ImmutableSet.of(NullType.get());

  private Set<DataType> notSupportSparkTypes = ImmutableSet.of(DataTypes.BinaryType);

  @BeforeAll
  void init() {
    gravitinoToSparkTypeMapper.put(IntegerType.get(), DataTypes.IntegerType);
    gravitinoToSparkTypeMapper.put(BooleanType.get(), DataTypes.BooleanType);
    gravitinoToSparkTypeMapper.put(StringType.get(), DataTypes.StringType);
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
