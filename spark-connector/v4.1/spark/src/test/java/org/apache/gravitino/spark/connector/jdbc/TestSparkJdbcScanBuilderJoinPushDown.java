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

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScanBuilder;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.HashMap;

public class TestSparkJdbcScanBuilderJoinPushDown {

  private static final String URL = "jdbc:postgresql://localhost/test";
  private static final StructType SCHEMA =
      new StructType().add("id", DataTypes.IntegerType).add("s", DataTypes.StringType);

  @Test
  void testJoinsOfGravitinoTablesStayInSpark() {
    JDBCScanBuilder left = jdbcScanBuilder("a");
    JDBCScanBuilder right = jdbcScanBuilder("b");
    // Spark's own builders accept each other as join sides: both read the same PostgreSQL source.
    Assertions.assertTrue(left.isOtherSideCompatibleForJoin(right));

    SparkJdbcPushDownPolicy policy =
        new SparkJdbcPushDownPolicy(
            new Column[] {
              Column.of("id", Types.IntegerType.get()), Column.of("s", Types.StringType.get())
            },
            SCHEMA,
            SCHEMA,
            JdbcDialects.get(URL),
            URL);
    SparkJdbcScanBuilder gravitinoLeft = new SparkJdbcScanBuilder(left, policy);
    Assertions.assertFalse(
        gravitinoLeft.isOtherSideCompatibleForJoin(new SparkJdbcScanBuilder(right, policy)));
    Assertions.assertFalse(gravitinoLeft.isOtherSideCompatibleForJoin(right));
  }

  private static JDBCScanBuilder jdbcScanBuilder(String table) {
    JDBCOptions options =
        new JDBCOptions(
            new HashMap<String, String>()
                .updated("url", URL)
                .updated("dbtable", table)
                .updated("driver", "org.postgresql.Driver"));
    return new JDBCScanBuilder(
        mock(SparkSession.class, RETURNS_DEEP_STUBS),
        SCHEMA,
        options,
        new HashMap<String, SQLMetric>());
  }
}
