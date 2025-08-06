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
package org.apache.gravitino.stats;

import java.util.Map;

/**
 * PartitionUpdateStatistics represents the statistics for a specific partition that can be updated.
 * It contains the partition name and a map of statistic names to their values.
 */
public class PartitionStatisticsUpdate {

  private final String partitionName;
  private final Map<String, StatisticValue<?>> statistics;

  /**
   * Creates a PartitionUpdateStatistics instance with the specified partition name and statistics.
   *
   * @param partitionName the name of the partition
   * @param statistics a map of statistic names to their values to be updated
   * @return a PartitionUpdateStatistics instance
   */
  public static PartitionStatisticsUpdate of(
      String partitionName, Map<String, StatisticValue<?>> statistics) {
    return new PartitionStatisticsUpdate(partitionName, statistics);
  }

  private PartitionStatisticsUpdate(
      String partitionName, Map<String, StatisticValue<?>> statistics) {
    this.partitionName = partitionName;
    this.statistics = statistics;
  }

  /**
   * Returns the name of the partition for which these statistics are applicable.
   *
   * @return the name of the partition.
   */
  public String partitionName() {
    return partitionName;
  }

  /**
   * Returns the statistics to be updated for the partition.
   *
   * @return a map where the key is the statistic name and the value is the statistic value.
   */
  public Map<String, StatisticValue<?>> statistics() {
    return statistics;
  }
}
