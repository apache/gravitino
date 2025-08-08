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
package org.apache.gravitino.stats.storage;

import java.util.Map;
import org.apache.gravitino.stats.StatisticValue;

/** Represents a collection of statistics for a specific partition in a metadata object. */
public class PersistedPartitionStatistics {

  private final String partitionName;
  private final Map<String, StatisticValue<?>> statistics;

  /**
   * Creates an instance of {@link PersistedPartitionStatistics}.
   *
   * @param partitionName the name of the partition for which these statistics are applicable
   * @param statistics a map of statistics applicable to the partition, where the key is the
   *     statistic name
   * @return a new instance of {@link PersistedPartitionStatistics}
   */
  public static PersistedPartitionStatistics of(
      String partitionName, Map<String, StatisticValue<?>> statistics) {
    return new PersistedPartitionStatistics(partitionName, statistics);
  }

  /**
   * Private constructor for {@link PersistedPartitionStatistics}.
   *
   * @param partitionName the name of the partition for which these statistics are applicable
   * @param statistics a map of statistics applicable to the partition, where the key is the
   *     statistic name
   */
  private PersistedPartitionStatistics(
      String partitionName, Map<String, StatisticValue<?>> statistics) {
    this.partitionName = partitionName;
    this.statistics = statistics;
  }

  /**
   * Returns the name of the partition for which these statistics are applicable.
   *
   * @return the name of the partition
   */
  public String partitionName() {
    return partitionName;
  }

  /**
   * Returns the statistics for the partition.
   *
   * @return a map of statistics applicable to the partition, where the key is the statistic name
   */
  public Map<String, StatisticValue<?>> statistics() {
    return statistics;
  }
}
