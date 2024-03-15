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
import com.datastrato.gravitino.rel.types.Types.ListType;
import com.datastrato.gravitino.rel.types.Types.LongType;
import com.datastrato.gravitino.rel.types.Types.MapType;
import com.datastrato.gravitino.rel.types.Types.NullType;
import com.datastrato.gravitino.rel.types.Types.ShortType;
import com.datastrato.gravitino.rel.types.Types.StringType;
import com.datastrato.gravitino.rel.types.Types.StructType;
import com.datastrato.gravitino.rel.types.Types.TimestampType;
import com.datastrato.gravitino.rel.types.Types.VarCharType;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Set;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestSparkTypeConverter {

  private HashMap<Type, DataType> gravitinoToSparkTypeMapper = new HashMap<>();

  private StructType gravitinoStructType =
      StructType.of(
          StructType.Field.of("col1", IntegerType.get(), true, null),
          StructType.Field.of("col2", StringType.get(), true, null),
          StructType.Field.of(
              "col3",
              StructType.of(
                  StructType.Field.of("col3_1", IntegerType.get(), true, null),
                  StructType.Field.of("col3_2", StringType.get(), true, null)),
              true,
              null),
          StructType.Field.of(
              "col4", MapType.of(IntegerType.get(), StringType.get(), true), true, null),
          StructType.Field.of("col5", ListType.of(IntegerType.get(), true), true, null),
          StructType.Field.of("col6", IntegerType.get(), false, null),
          StructType.Field.of("col7", IntegerType.get(), true, "This is a comment"));

  private org.apache.spark.sql.types.StructType sparkStructType =
      DataTypes.createStructType(
          new org.apache.spark.sql.types.StructField[] {
            DataTypes.createStructField("col1", DataTypes.IntegerType, true),
            DataTypes.createStructField("col2", DataTypes.StringType, true),
            DataTypes.createStructField(
                "col3",
                DataTypes.createStructType(
                    new org.apache.spark.sql.types.StructField[] {
                      DataTypes.createStructField("col3_1", DataTypes.IntegerType, true),
                      DataTypes.createStructField("col3_2", DataTypes.StringType, true)
                    }),
                true),
            DataTypes.createStructField(
                "col4",
                DataTypes.createMapType(DataTypes.IntegerType, DataTypes.StringType, true),
                true),
            DataTypes.createStructField(
                "col5", DataTypes.createArrayType(DataTypes.IntegerType), true),
            DataTypes.createStructField("col6", DataTypes.IntegerType, false),
            DataTypes.createStructField(
                "col7",
                DataTypes.IntegerType,
                true,
                new MetadataBuilder()
                    .putString(ConnectorConstants.COMMENT, "This is a comment")
                    .build())
          });

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
    gravitinoToSparkTypeMapper.put(
        ListType.of(IntegerType.get(), true), DataTypes.createArrayType(DataTypes.IntegerType));
    gravitinoToSparkTypeMapper.put(
        MapType.of(IntegerType.get(), StringType.get(), true),
        DataTypes.createMapType(DataTypes.IntegerType, DataTypes.StringType));
  }

  private boolean checkGravitinoStructTypeConvertToSpark(Type gravitinoType, DataType sparkType) {
    if (gravitinoType instanceof StructType
        && sparkType instanceof org.apache.spark.sql.types.StructType) {
      StructType gravitinoStructType = (StructType) gravitinoType;
      org.apache.spark.sql.types.StructType sparkStructType =
          (org.apache.spark.sql.types.StructType) sparkType;

      if (gravitinoStructType.fields().length != sparkStructType.fields().length) {
        return false;
      }

      return java.util.stream.IntStream.range(0, gravitinoStructType.fields().length)
          .allMatch(
              i -> {
                String gravitinoComment =
                    gravitinoStructType.fields()[i].comment() == null
                        ? ""
                        : gravitinoStructType.fields()[i].comment();
                String sparkComment =
                    sparkStructType.fields()[i].metadata().contains(ConnectorConstants.COMMENT)
                        ? sparkStructType
                            .fields()[i]
                            .metadata()
                            .getString(ConnectorConstants.COMMENT)
                        : "";
                return gravitinoStructType
                        .fields()[i]
                        .name()
                        .equals(sparkStructType.fields()[i].name())
                    && gravitinoStructType.fields()[i].nullable()
                        == sparkStructType.fields()[i].nullable()
                    && checkGravitinoStructTypeConvertToSpark(
                        gravitinoStructType.fields()[i].type(),
                        sparkStructType.fields()[i].dataType())
                    && gravitinoComment.equals(sparkComment);
              });
    } else if (gravitinoType instanceof ListType
        && sparkType instanceof org.apache.spark.sql.types.ArrayType) {
      ListType gravitinoArrayType = (ListType) gravitinoType;
      org.apache.spark.sql.types.ArrayType sparkArrayType =
          (org.apache.spark.sql.types.ArrayType) sparkType;

      return gravitinoArrayType.elementNullable() == sparkArrayType.containsNull()
          && checkGravitinoStructTypeConvertToSpark(
              gravitinoArrayType.elementType(), sparkArrayType.elementType());
    } else if (gravitinoType instanceof MapType
        && sparkType instanceof org.apache.spark.sql.types.MapType) {
      MapType gravitinoMapType = (MapType) gravitinoType;
      org.apache.spark.sql.types.MapType sparkMapType =
          (org.apache.spark.sql.types.MapType) sparkType;

      return gravitinoMapType.valueNullable() == sparkMapType.valueContainsNull()
          && checkGravitinoStructTypeConvertToSpark(
              gravitinoMapType.keyType(), sparkMapType.keyType())
          && checkGravitinoStructTypeConvertToSpark(
              gravitinoMapType.valueType(), sparkMapType.valueType());
    } else {
      return gravitinoType.simpleString().equalsIgnoreCase(sparkType.typeName());
    }
  }

  private boolean checkSparkStructTypeConvertToGravitino(
      StructType gravitinoType, org.apache.spark.sql.types.StructType sparkType) {
    if (gravitinoType.fields().length != sparkType.fields().length) {
      return false;
    }
    return java.util.stream.IntStream.range(0, gravitinoType.fields().length)
        .allMatch(
            i -> {
              String sparkComment =
                  sparkType.fields()[i].metadata().contains(ConnectorConstants.COMMENT)
                      ? sparkType.fields()[i].metadata().getString(ConnectorConstants.COMMENT)
                      : "";
              String gravitinoComment =
                  gravitinoType.fields()[i].comment() == null
                      ? ""
                      : gravitinoType.fields()[i].comment();
              return gravitinoType.fields()[i].name().equals(sparkType.fields()[i].name())
                  && gravitinoType.fields()[i].nullable() == sparkType.fields()[i].nullable()
                  && gravitinoType
                      .fields()[i]
                      .type()
                      .equals(SparkTypeConverter.toGravitinoType(sparkType.fields()[i].dataType()))
                  && gravitinoComment.equals(sparkComment);
            });
  }

  @Test
  void testConvertGravitinoTypeToSpark() {
    gravitinoToSparkTypeMapper.forEach(
        (gravitinoType, sparkType) ->
            Assertions.assertEquals(sparkType, SparkTypeConverter.toSparkType(gravitinoType)));

    Assertions.assertTrue(
        checkGravitinoStructTypeConvertToSpark(gravitinoStructType, sparkStructType));

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

    Assertions.assertTrue(
        checkSparkStructTypeConvertToGravitino(gravitinoStructType, sparkStructType));

    notSupportSparkTypes.forEach(
        sparkType ->
            Assertions.assertThrowsExactly(
                UnsupportedOperationException.class,
                () -> SparkTypeConverter.toGravitinoType(sparkType)));
  }
}
