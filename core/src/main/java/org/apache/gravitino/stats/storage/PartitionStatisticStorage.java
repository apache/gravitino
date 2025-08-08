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

import com.google.common.collect.Lists;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatisticsDrop;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;

/** Interface for managing partition statistics in a storage system. */
public interface PartitionStatisticStorage extends Closeable {

  /**
   * Returns the name of the metalake associated with this storage.
   *
   * @return the name of the metalake
   */
  String metalake();

  /**
   * Returns the properties of this storage.
   *
   * @return a map of properties associated with this storage.
   */
  Map<String, String> properties();

  /**
   * Lists statistics for a given metadata object within a specified range of partition names.
   *
   * @param metadataObject the metadata object for which statistics are being listed
   * @param partitionRange the range of partition names for which statistics are being listed
   * @return a map where the key is the partition name and the value is another map * containing
   *     statistic names and their corresponding values.
   */
  Map<String, Map<String, StatisticValue<?>>> listStatistics(
      MetadataObject metadataObject, PartitionRange partitionRange);

  /**
   * Lists statistics for a given metadata object and specific partition names. This interface may
   * be used in the future. The upper logic layer won't call this method now.
   *
   * @param metadataObject the metadata object for which statistics are being listed
   * @param partitionNames a list of partition names for which statistics are being listed
   * @return a map where the key is the partition name and the value is another map.
   */
  default Map<String, Map<String, StatisticValue<?>>> listStatistics(
      MetadataObject metadataObject, List<String> partitionNames) {
    throw new UnsupportedOperationException(
        "Don't support listStatistics with partition names yet.");
  }

  /**
   * Appends statistics to the storage for a given metadata object.
   *
   * @param statisticsToAppend a map where the key is a {@link MetadataObject} and the value is a
   *     {@link PartitionStatisticsUpdate} object
   */
  void appendStatistics(Map<MetadataObject, List<PartitionStatisticsUpdate>> statisticsToAppend);

  /**
   * Drops statistics for specified partitions of a metadata object.
   *
   * @param partitionStatisticsToDrop a map where the key is a {@link MetadataObject} and the value
   *     is a list of {@link PartitionStatisticsDrop} objects representing the partitions and their
   *     statistics to be dropped.
   */
  void dropStatistics(Map<MetadataObject, List<PartitionStatisticsDrop>> partitionStatisticsToDrop);

  /**
   * Updates statistics for a given metadata object. The default implementation is to first drop and
   * then append the statistics. Developer can override this logic if needed.
   *
   * @param statisticsToUpdate a map where the key is a {@link MetadataObject} and the value is
   *     another map containing partition names as keys and their corresponding statistics as
   *     values.
   */
  default void updateStatistics(
      Map<MetadataObject, List<PartitionStatisticsUpdate>> statisticsToUpdate) {
    Map<MetadataObject, List<PartitionStatisticsDrop>> statisticsToDrop =
        statisticsToUpdate.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        e.getValue().stream()
                            .map(
                                entry ->
                                    PartitionStatisticsDrop.of(
                                        entry.partitionName(),
                                        Lists.newArrayList(entry.statistics().keySet())))
                            .collect(Collectors.toList())));
    dropStatistics(statisticsToDrop);
    appendStatistics(statisticsToUpdate);
  }
}
