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
package org.apache.gravitino.spark.connector.jdbc;

import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link SparkJdbcTypeConverter34}. */
public class TestSparkJdbcTypeConverter34 {

  private final SparkJdbcTypeConverter34 sparkJdbcTypeConverter34 = new SparkJdbcTypeConverter34();

  @Test
  void testConvertTimestampTypesToSparkTimestamp() {
    Assertions.assertEquals(
        DataTypes.TimestampType,
        sparkJdbcTypeConverter34.toSparkType(Types.TimestampType.withoutTimeZone()));
    Assertions.assertEquals(
        DataTypes.TimestampType,
        sparkJdbcTypeConverter34.toSparkType(Types.TimestampType.withTimeZone()));
  }

  @Test
  void testConvertVarCharTypeToSparkString() {
    Assertions.assertEquals(
        DataTypes.StringType, sparkJdbcTypeConverter34.toSparkType(Types.VarCharType.of(10)));
  }

  @Test
  void testConvertExternalTypeToSparkString() {
    Assertions.assertEquals(
        DataTypes.StringType,
        sparkJdbcTypeConverter34.toSparkType(Types.ExternalType.of("LARGEINT")));
    Assertions.assertEquals(
        DataTypes.StringType,
        sparkJdbcTypeConverter34.toSparkType(Types.ExternalType.of("BITMAP")));
    Assertions.assertEquals(
        DataTypes.StringType, sparkJdbcTypeConverter34.toSparkType(Types.ExternalType.of("IPv4")));
  }
}
