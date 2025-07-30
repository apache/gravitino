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
package org.apache.gravitino.storage.external.stats;

import com.google.common.collect.Lists;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.stats.PartitionDropStatistics;
import org.apache.gravitino.stats.PartitionUpdateStatistics;
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
   * @param fromPartitionName the starting partition name (inclusive)
   * @param toPartitionName the ending partition name (exclusive)
   * @return a map where the key is the partition name and the value is another map * containing
   *     statistic names and their corresponding values.
   */
  Map<String, Map<String, StatisticValue<?>>> listStatistics(
      MetadataObject metadataObject, String fromPartitionName, String toPartitionName);

  /**
   * Lists statistics for a given metadata object and specific partition names.
   *
   * @param metadataObject the metadata object for which statistics are being listed
   * @param partitionNames a list of partition names for which statistics are being listed
   * @return a map where the key is the partition name and the value is another map.
   */
  Map<String, Map<String, StatisticValue<?>>> listStatistics(
      MetadataObject metadataObject, List<String> partitionNames);

  /**
   * Appends statistics to the storage for a given metadata object.
   *
   * @param statisticsToAppend a map where the key is a {@link MetadataObject} and the value is a
   *     {@link PartitionUpdateStatistics} object
   */
  void appendStatistics(Map<MetadataObject, List<PartitionUpdateStatistics>> statisticsToAppend);

  /**
   * Drops statistics for specified partitions of a metadata object.
   *
   * @param partitionStatisticsToDrop a map where the key is a {@link MetadataObject} and the value
   *     is a list of {@link PartitionDropStatistics}
   */
  void dropStatistics(Map<MetadataObject, List<PartitionDropStatistics>> partitionStatisticsToDrop);

  /**
   * Updates statistics for a given metadata object. The default implementation is to first drop and
   * then append the statistics. Developer can override this logic if needed.
   *
   * @param statisticsToUpdate a map where the key is a {@link MetadataObject} and the value is
   *     another map containing partition names as keys and their corresponding statistics as
   *     values.
   */
  default void updateStatistics(
      Map<MetadataObject, List<PartitionUpdateStatistics>> statisticsToUpdate) {
    Map<MetadataObject, List<PartitionDropStatistics>> statisticsToDrop =
        statisticsToUpdate.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        e.getValue().stream()
                            .map(
                                entry ->
                                    PartitionDropStatistics.of(
                                        entry.partitionName(),
                                        Lists.newArrayList(entry.statistics().keySet())))
                            .collect(Collectors.toList())));
    dropStatistics(statisticsToDrop);
    appendStatistics(statisticsToUpdate);
  }
}
