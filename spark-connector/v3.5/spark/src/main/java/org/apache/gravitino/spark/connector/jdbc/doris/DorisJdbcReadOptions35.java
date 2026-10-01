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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Validated Spark JDBC options for the Doris SQL lane. */
final class DorisJdbcReadOptions35 {

  private final Map<String, String> options;

  private DorisJdbcReadOptions35(Map<String, String> options) {
    this.options = Collections.unmodifiableMap(new LinkedHashMap<>(options));
  }

  static DorisJdbcReadOptions35 from(Map<String, String> properties) {
    Map<String, String> source = properties == null ? Collections.emptyMap() : properties;
    String partitionColumn =
        trimToNull(source.get(DorisConnectorConstants35.JDBC_PARTITION_COLUMN));
    String lowerBound = trimToNull(source.get(DorisConnectorConstants35.JDBC_LOWER_BOUND));
    String upperBound = trimToNull(source.get(DorisConnectorConstants35.JDBC_UPPER_BOUND));
    String numPartitions = trimToNull(source.get(DorisConnectorConstants35.JDBC_NUM_PARTITIONS));
    int count = countPresent(partitionColumn, lowerBound, upperBound, numPartitions);
    if (count != 0 && count != 4) {
      throw new IllegalArgumentException(
          "Doris JDBC partitioning requires partition column, lower bound, upper bound, and "
              + "number of partitions together");
    }
    Map<String, String> result = new LinkedHashMap<>();
    if (count == 4) {
      requirePositive(DorisConnectorConstants35.JDBC_NUM_PARTITIONS, numPartitions);
      result.put("partitionColumn", partitionColumn);
      result.put("lowerBound", lowerBound);
      result.put("upperBound", upperBound);
      result.put("numPartitions", numPartitions);
    }
    String fetchSize = trimToNull(source.get(DorisConnectorConstants35.JDBC_FETCH_SIZE));
    if (fetchSize != null) {
      requirePositive(DorisConnectorConstants35.JDBC_FETCH_SIZE, fetchSize);
      result.put("fetchsize", fetchSize);
    }
    return new DorisJdbcReadOptions35(result);
  }

  Map<String, String> asSparkOptions() {
    return options;
  }

  private static int countPresent(String... values) {
    int count = 0;
    for (String value : values) {
      if (value != null) {
        count++;
      }
    }
    return count;
  }

  private static String trimToNull(String value) {
    return value == null || value.trim().isEmpty() ? null : value.trim();
  }

  private static void requirePositive(String property, String value) {
    int parsed;
    try {
      parsed = Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(property + " must be a positive integer");
    }
    if (parsed < 1) {
      throw new IllegalArgumentException(property + " must be a positive integer");
    }
  }
}
