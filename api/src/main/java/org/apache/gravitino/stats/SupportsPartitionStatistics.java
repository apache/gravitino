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
import org.apache.gravitino.annotation.Unstable;
import org.apache.gravitino.exceptions.UnmodifiableStatisticException;

/** SupportsPartitionStatistics provides methods to list and update statistics for partitions. */
@Unstable
public interface SupportsPartitionStatistics {

  /**
   * Lists statistics for partitions from one partition name to another partition name.
   *
   * @param range the range of partitions to list statistics for, which can be defined by.
   * @return a list of PartitionStatistics, where each PartitionStatistics contains the partition
   *     name and a list of statistics applicable to that partition.
   */
  List<PartitionStatistics> listPartitionStatistics(PartitionRange range);

  /**
   * Updates statistics with the provided values. If the statistic exists, it will be updated with
   * the new value. If the statistic does not exist, it will be created. If the statistic is
   * unmodifiable, it will throw an UnmodifiableStatisticException.
   *
   * @param statisticsToUpdate a list of PartitionUpdateStatistics, where each
   *     PartitionStatisticsUpdate contains the partition name and a map of statistic names to their
   *     values to be updated.
   * @throws UnmodifiableStatisticException if any of the statistics to be updated are unmodifiable
   */
  void updatePartitionStatistics(List<PartitionStatisticsUpdate> statisticsToUpdate)
      throws UnmodifiableStatisticException;

  /**
   * Drops statistics for the specified partitions. If the statistic is unmodifiable, it will throw
   * an UnmodifiableStatisticException.
   *
   * @param statisticsToDrop a list of PartitionStatisticsDrop, where each PartitionStatisticsDrop
   * @return true if the statistics were successfully dropped.
   * @throws UnmodifiableStatisticException if any of the statistics to be dropped are unmodifiable
   */
  boolean dropPartitionStatistics(List<PartitionStatisticsDrop> statisticsToDrop)
      throws UnmodifiableStatisticException;
}
