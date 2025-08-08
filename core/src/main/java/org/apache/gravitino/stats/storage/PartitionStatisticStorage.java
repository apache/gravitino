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
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatisticsDrop;

/** Interface for managing partition statistics in a storage system. */
public interface PartitionStatisticStorage extends Closeable {

  /**
   * Lists statistics for a given metadata object within a specified range of partition names. The
   * implementation should guarantee the thread safe.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object for which statistics are being listed
   * @param partitionRange the range of partition names for which statistics are being listed
   * @return a list of {@link PersistedPartitionStatistics} objects, each containing the partition
   *     name
   */
  List<PersistedPartitionStatistics> listStatistics(
      String metalake, MetadataObject metadataObject, PartitionRange partitionRange);

  /**
   * Lists statistics for a given metadata object and specific partition names. This interface may
   * be used in the future. The upper logic layer won't call this method now. The implementation
   * should guarantee the thread safe.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object for which statistics are being listed
   * @param partitionNames a list of partition names for which statistics are being listed
   * @return a list of {@link PersistedPartitionStatistics} objects, each containing the partition
   *     name
   */
  default List<PersistedPartitionStatistics> listStatistics(
      String metalake, MetadataObject metadataObject, List<String> partitionNames) {
    throw new UnsupportedOperationException(
        "Don't support listStatistics with partition names yet.");
  }

  /**
   * Appends statistics to the storage for a given metadata object. The implementation should
   * guarantee the thread safe.
   *
   * @param metalake the name of the metalake
   * @param statisticsToAppend a list of {@link MetadataObjectStatisticsUpdate} objects, each
   *     containing the metadata object and its associated statistics updates.
   */
  void appendStatistics(String metalake, List<MetadataObjectStatisticsUpdate> statisticsToAppend);

  /**
   * Drops statistics for specified partitions of a metadata object. The implementation should
   * guarantee the thread safe.
   *
   * @param metalake the name of the metalake
   * @param partitionStatisticsToDrop a map where the key is a {@link MetadataObject} and the value
   *     is a list of {@link PartitionStatisticsDrop}
   */
  void dropStatistics(
      String metalake, List<MetadataObjectStatisticsDrop> partitionStatisticsToDrop);

  /**
   * Updates statistics for a given metadata object. The default implementation is to first drop and
   * then append the statistics. Developer can override this logic if needed. The implementation
   * should guarantee the thread safe.
   *
   * @param metalake the name of the metalake
   * @param statisticsToUpdate a list of {@link MetadataObjectStatisticsUpdate} objects, each
   *     containing the metadata object and its associated statistics updates.
   */
  default void updateStatistics(
      String metalake, List<MetadataObjectStatisticsUpdate> statisticsToUpdate) {
    List<MetadataObjectStatisticsDrop> statisticsToDrop =
        statisticsToUpdate.stream()
            .map(
                update ->
                    MetadataObjectStatisticsDrop.of(
                        update.metadataObject(),
                        update.partitionUpdates().stream()
                            .map(
                                partitionUpdate ->
                                    PartitionStatisticsDrop.of(
                                        partitionUpdate.partitionName(),
                                        Lists.newArrayList(partitionUpdate.statistics().keySet())))
                            .collect(Collectors.toList())))
            .collect(Collectors.toList());

    dropStatistics(metalake, statisticsToDrop);
    appendStatistics(metalake, statisticsToUpdate);
  }
}
