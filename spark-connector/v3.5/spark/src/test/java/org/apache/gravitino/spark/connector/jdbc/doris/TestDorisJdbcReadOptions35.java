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
package org.apache.gravitino.spark.connector.jdbc.doris;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests validation of the Spark JDBC SQL-lane options. */
public class TestDorisJdbcReadOptions35 {

  @Test
  void testPartitionAndFetchOptions() {
    DorisJdbcReadOptions35 options =
        DorisJdbcReadOptions35.from(
            ImmutableMap.of(
                DorisConnectorConstants35.JDBC_PARTITION_COLUMN,
                "id",
                DorisConnectorConstants35.JDBC_LOWER_BOUND,
                "1",
                DorisConnectorConstants35.JDBC_UPPER_BOUND,
                "100",
                DorisConnectorConstants35.JDBC_NUM_PARTITIONS,
                "4",
                DorisConnectorConstants35.JDBC_FETCH_SIZE,
                "500"));

    Map<String, String> expected =
        ImmutableMap.of(
            "partitionColumn", "id",
            "lowerBound", "1",
            "upperBound", "100",
            "numPartitions", "4",
            "fetchsize", "500");
    assertEquals(expected, options.asSparkOptions());
  }

  @Test
  void testPartialPartitionOptionsAreRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisJdbcReadOptions35.from(
                ImmutableMap.of(DorisConnectorConstants35.JDBC_PARTITION_COLUMN, "id")));
  }

  @Test
  void testNonPositiveNumericOptionsAreRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisJdbcReadOptions35.from(
                ImmutableMap.of(DorisConnectorConstants35.JDBC_FETCH_SIZE, "0")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisJdbcReadOptions35.from(
                ImmutableMap.of(
                    DorisConnectorConstants35.JDBC_PARTITION_COLUMN,
                    "id",
                    DorisConnectorConstants35.JDBC_LOWER_BOUND,
                    "1",
                    DorisConnectorConstants35.JDBC_UPPER_BOUND,
                    "100",
                    DorisConnectorConstants35.JDBC_NUM_PARTITIONS,
                    "not-a-number")));
  }
}
