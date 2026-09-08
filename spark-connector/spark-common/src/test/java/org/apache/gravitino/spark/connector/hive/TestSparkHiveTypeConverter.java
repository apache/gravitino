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
package org.apache.gravitino.spark.connector.hive;

import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link SparkHiveTypeConverter}. */
public class TestSparkHiveTypeConverter {

  private final SparkTypeConverter sparkHiveTypeConverter = new SparkHiveTypeConverter();

  @Test
  void testTimestampLosesTimeZone() {
    // Hive stores no timezone, so a Spark TimestampType has to come back without one.
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(),
        sparkHiveTypeConverter.toGravitinoType(DataTypes.TimestampType));
    Assertions.assertEquals(
        DataTypes.TimestampType,
        sparkHiveTypeConverter.toSparkType(Types.TimestampType.withoutTimeZone()));
    Assertions.assertEquals(
        DataTypes.TimestampType,
        sparkHiveTypeConverter.toSparkType(Types.TimestampType.withTimeZone()));
  }

  @Test
  void testTimestampNTZIsRejected() {
    // The base converter maps TimestampNTZType to a timezone-less Gravitino type, which would be
    // indistinguishable from a plain Hive timestamp; reject it instead of silently accepting.
    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> sparkHiveTypeConverter.toGravitinoType(DataTypes.TimestampNTZType));
    Assertions.assertTrue(exception.getMessage().contains("Hive does not support 'timestamp_ntz'"));
  }

  @Test
  void testOtherTypesDelegateToTheBaseConverter() {
    Assertions.assertEquals(
        DataTypes.IntegerType, sparkHiveTypeConverter.toSparkType(Types.IntegerType.get()));
    Assertions.assertEquals(
        Types.VarCharType.of(10), sparkHiveTypeConverter.toGravitinoType(VarcharType.apply(10)));
  }
}
