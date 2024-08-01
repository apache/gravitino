/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.spark.connector;

import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Set;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types.BinaryType;
import org.apache.gravitino.rel.types.Types.BooleanType;
import org.apache.gravitino.rel.types.Types.ByteType;
import org.apache.gravitino.rel.types.Types.DateType;
import org.apache.gravitino.rel.types.Types.DecimalType;
import org.apache.gravitino.rel.types.Types.DoubleType;
import org.apache.gravitino.rel.types.Types.FixedCharType;
import org.apache.gravitino.rel.types.Types.FloatType;
import org.apache.gravitino.rel.types.Types.IntegerType;
import org.apache.gravitino.rel.types.Types.ListType;
import org.apache.gravitino.rel.types.Types.LongType;
import org.apache.gravitino.rel.types.Types.MapType;
import org.apache.gravitino.rel.types.Types.NullType;
import org.apache.gravitino.rel.types.Types.ShortType;
import org.apache.gravitino.rel.types.Types.StringType;
import org.apache.gravitino.rel.types.Types.StructType;
import org.apache.gravitino.rel.types.Types.TimestampType;
import org.apache.gravitino.rel.types.Types.VarCharType;
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
  private Set<Type> notSupportGravitinoTypes = ImmutableSet.of();

  private Set<DataType> notSupportSparkTypes = ImmutableSet.of();
  private SparkTypeConverter sparkTypeConverter;

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
    gravitinoToSparkTypeMapper.put(
        ListType.of(IntegerType.get(), true), DataTypes.createArrayType(DataTypes.IntegerType));
    gravitinoToSparkTypeMapper.put(
        MapType.of(IntegerType.get(), StringType.get(), true),
        DataTypes.createMapType(DataTypes.IntegerType, DataTypes.StringType));
    gravitinoToSparkTypeMapper.put(createGravitinoStructType(), createSparkStructType());
    gravitinoToSparkTypeMapper.put(NullType.get(), DataTypes.NullType);

    this.sparkTypeConverter = new SparkTypeConverter();
  }

  @Test
  void testConvertGravitinoTypeToSpark() {
    gravitinoToSparkTypeMapper.forEach(
        (gravitinoType, sparkType) ->
            Assertions.assertEquals(sparkType, sparkTypeConverter.toSparkType(gravitinoType)));

    notSupportGravitinoTypes.forEach(
        gravitinoType ->
            Assertions.assertThrowsExactly(
                UnsupportedOperationException.class,
                () -> sparkTypeConverter.toSparkType(gravitinoType)));
  }

  @Test
  void testConvertSparkTypeToGravitino() {
    gravitinoToSparkTypeMapper.forEach(
        (gravitinoType, sparkType) ->
            Assertions.assertEquals(gravitinoType, sparkTypeConverter.toGravitinoType(sparkType)));

    notSupportSparkTypes.forEach(
        sparkType ->
            Assertions.assertThrowsExactly(
                UnsupportedOperationException.class,
                () -> sparkTypeConverter.toGravitinoType(sparkType)));
  }

  /** Create a Gravitino StructType for testing. */
  private static StructType createGravitinoStructType() {
    return StructType.of(
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
  }

  /** Create a Spark StructType for testing. */
  private static org.apache.spark.sql.types.StructType createSparkStructType() {
    return DataTypes.createStructType(
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
  }
}
