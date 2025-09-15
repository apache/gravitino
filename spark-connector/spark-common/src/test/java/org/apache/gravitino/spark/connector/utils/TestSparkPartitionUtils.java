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

package org.apache.gravitino.spark.connector.utils;

import java.time.LocalDate;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VarcharType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestSparkPartitionUtils {

  boolean boolValue = true;
  byte byteValue = 100;
  short shortValue = 1000;
  int intValue = 100000;
  long longValue = 10000000000L;
  float floatValue = 3.14f;
  double doubleValue = 3.1415926535;
  UTF8String stringValue = UTF8String.fromString("Hello World");
  UTF8String varcharValue = UTF8String.fromString("abc");
  UTF8String charValue = UTF8String.fromString("xy");
  int date = 0;
  private InternalRow internalRow =
      new GenericInternalRow(
          new Object[] {
            boolValue,
            byteValue,
            shortValue,
            intValue,
            longValue,
            floatValue,
            doubleValue,
            stringValue,
            date,
            varcharValue,
            charValue,
          });
  private Literal[] literals =
      new Literals.LiteralImpl[] {
        Literals.booleanLiteral(boolValue),
        Literals.byteLiteral(byteValue),
        Literals.shortLiteral(shortValue),
        Literals.integerLiteral(intValue),
        Literals.longLiteral(longValue),
        Literals.floatLiteral(floatValue),
        Literals.doubleLiteral(doubleValue),
        Literals.stringLiteral(stringValue.toString()),
        Literals.dateLiteral(LocalDate.of(1970, 1, 1)),
        Literals.varcharLiteral(5, varcharValue.toString()),
        Literals.of(charValue, Types.FixedCharType.of(2)),
      };
  private String[] hivePartitionValues = {
    "true",
    "100",
    "1000",
    "100000",
    "10000000000",
    "3.14",
    "3.1415926535",
    "Hello World",
    "1970-01-01",
    "abc",
    "xy"
  };
  StructType schema =
      new StructType(
          new StructField[] {
            new StructField("boolean", DataTypes.BooleanType, false, Metadata.empty()),
            new StructField("byte", DataTypes.ByteType, false, Metadata.empty()),
            new StructField("short", DataTypes.ShortType, false, Metadata.empty()),
            new StructField("int", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("long", DataTypes.LongType, false, Metadata.empty()),
            new StructField("float", DataTypes.FloatType, false, Metadata.empty()),
            new StructField("double", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("string", DataTypes.StringType, false, Metadata.empty()),
            new StructField("date", DataTypes.DateType, false, Metadata.empty()),
            new StructField("varchar5", VarcharType.apply(5), false, Metadata.empty()),
            new StructField("char2", CharType.apply(2), false, Metadata.empty()),
          });

  @Test
  void testToGravitinoLiteral() {
    int numFields = internalRow.numFields();
    for (int i = 0; i < numFields; i++) {
      DataType dataType = schema.apply(i).dataType();
      Assertions.assertEquals(
          literals[i], SparkPartitionUtils.toGravitinoLiteral(internalRow, i, dataType));
    }

    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class,
        () ->
            SparkPartitionUtils.toGravitinoLiteral(
                new GenericInternalRow(new Object[] {"1970-01-01 00:00:00"}),
                0,
                DataTypes.TimestampType));
  }

  @Test
  void testGetPartitionValueAsString() {
    int numFields = internalRow.numFields();
    for (int i = 0; i < numFields; i++) {
      DataType dataType = schema.apply(i).dataType();
      Assertions.assertEquals(
          hivePartitionValues[i],
          SparkPartitionUtils.getPartitionValueAsString(internalRow, i, dataType));
    }

    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class,
        () ->
            SparkPartitionUtils.getPartitionValueAsString(
                new GenericInternalRow(new Object[] {"1970-01-01 00:00:00"}),
                0,
                DataTypes.TimestampType));
  }

  @Test
  void testGetSparkPartitionValue() {
    int numFields = internalRow.numFields();
    for (int i = 0; i < numFields; i++) {
      DataType dataType = schema.apply(i).dataType();
      Assertions.assertEquals(
          internalRow.get(i, dataType),
          SparkPartitionUtils.getSparkPartitionValue(hivePartitionValues[i], dataType));
    }

    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class,
        () ->
            SparkPartitionUtils.getSparkPartitionValue(
                "1970-01-01 00:00:00", DataTypes.TimestampType));
  }

  @Test
  void testToGravitinoLiteralWithMixedNullAndNonNull() {
    GenericInternalRow mixedRow =
        new GenericInternalRow(new Object[] {null, UTF8String.fromString("test"), 42});

    Assertions.assertEquals(
        Literals.NULL, SparkPartitionUtils.toGravitinoLiteral(mixedRow, 0, DataTypes.StringType));

    Assertions.assertEquals(
        Literals.stringLiteral("test"),
        SparkPartitionUtils.toGravitinoLiteral(mixedRow, 1, DataTypes.StringType));

    Assertions.assertEquals(
        Literals.integerLiteral(42),
        SparkPartitionUtils.toGravitinoLiteral(mixedRow, 2, DataTypes.IntegerType));
  }
}
