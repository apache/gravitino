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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType$;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** JDBC schema lookup preserves distinct case-sensitive source columns. */
public class TestJdbcSchemaUtils {

  @Test
  void testExactColumnNamesWinRegardlessOfOrder() {
    StructType schema =
        new StructType().add("ts", TimestampNTZType$.MODULE$).add("TS", DataTypes.TimestampType);
    for (StructType ordered :
        new StructType[] {
          schema, new StructType().add(schema.fields()[1]).add(schema.fields()[0])
        }) {
      Assertions.assertEquals(TimestampNTZType$.MODULE$, JdbcSchemaUtils.fieldType(ordered, "ts"));
      Assertions.assertEquals(DataTypes.TimestampType, JdbcSchemaUtils.fieldType(ordered, "TS"));
    }
    // An exact match after multiple case-insensitive candidates must still win.
    Assertions.assertEquals(
        DataTypes.StringType,
        JdbcSchemaUtils.fieldType(schema.add("Ts", DataTypes.StringType), "Ts"));
  }

  @Test
  void testCaseInsensitiveFallbackMustBeUnique() {
    StructType schema = new StructType().add("ts", TimestampNTZType$.MODULE$);
    Assertions.assertEquals(TimestampNTZType$.MODULE$, JdbcSchemaUtils.fieldType(schema, "TS"));
    Assertions.assertNull(JdbcSchemaUtils.fieldType(schema, "missing"));
    Assertions.assertNull(
        JdbcSchemaUtils.fieldType(schema.add("TS", DataTypes.TimestampType), "Ts"));
  }
}
