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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.exceptions.UnmodifiableStatisticException;

/** SupportsPartitionStatistics provides methods to list and update statistics for partitions. */
interface SupportsPartitionStatistics {

  /**
   * Lists statistics for partitions from one partition name to another partition name.
   *
   * @param fromPartitionName Optional partition name to start listing from, inclusive.
   * @param toPartitionName Optional partition name to end listing at, exclusive.
   * @throws IllegalArgumentException if fromPartitionName and toPartitionName are empty at the same
   *     time or the toPartitionName partition key must be greater than fromPartitionName partition
   *     key.
   * @return a map where the key is the partition name and the value is a list of statistics
   */
  Map<String, List<Statistic>> listStatistics(
      Optional<String> fromPartitionName, Optional<String> toPartitionName)
      throws IllegalArgumentException;

  /**
   * Updates statistics with the provided values. If the statistic exists, it will be updated with
   * the new value. If the statistic does not exist, it will be created. If the statistic is
   * unmodifiable, it will throw an UnmodifiableStatisticException.
   *
   * @param statistics a map where the key is the partition name and the value is a map of statistic
   *     names to their values
   * @throws UnmodifiableStatisticException if any of the statistics to be updated are unmodifiable
   */
  void updateStatistics(Map<String, StatisticValue<?>> statistics)
      throws UnmodifiableStatisticException;

  /**
   * Drops statistics for the specified partitions. If the statistic is unmodifiable, it will throw
   * an UnmodifiableStatisticException.
   *
   * @param statistics a map where the key is the partition name and the value is a list of
   *     statistics to be dropped
   * @throws UnmodifiableStatisticException if any of the statistics to be dropped are unmodifiable
   */
  void dropStatistics(Map<String, List<Statistic>> statistics)
      throws UnmodifiableStatisticException;
}
