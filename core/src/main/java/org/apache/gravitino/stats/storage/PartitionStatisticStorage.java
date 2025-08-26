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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatisticsDrop;

/** Interface for managing partition statistics in a storage system. */
public interface PartitionStatisticStorage extends Closeable {

  /**
   * Lists statistics for a given metadata object within a specified range of partition names.
   * Locking guarantee: The upper layer will acquire a read lock at the metadata object level. For
   * example, if the metadata object is a table, the read lock of the table level will be held.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object for which statistics are being listed
   * @param partitionRange the range of partition names for which statistics are being listed
   * @return a list of {@link PersistedPartitionStatistics} objects, each containing the partition
   *     name
   */
  List<PersistedPartitionStatistics> listStatistics(
      String metalake, MetadataObject metadataObject, PartitionRange partitionRange)
      throws IOException;

  /**
   * Lists statistics for a given metadata object and specific partition names. This interface may
   * be reserved for the future use. The upper logic layer does not currently invoke this
   * implementation. Locking Guarantee: The upper layer will acquire a read lock at the metadata
   * object level. For example, if the metadata object is a table, the read lock of the table level
   * will be held.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object for which statistics are being listed
   * @param partitionNames a list of partition names for which statistics are being listed
   * @return a list of {@link PersistedPartitionStatistics} objects, each containing the partition
   *     name
   */
  default List<PersistedPartitionStatistics> listStatistics(
      String metalake, MetadataObject metadataObject, List<String> partitionNames)
      throws IOException {
    throw new UnsupportedOperationException(
        "Don't support listStatistics with partition names yet.");
  }

  /**
   * Drops statistics for specified partitions of a metadata object. Locking guarantee: The upper
   * layer will acquire a write lock at the metadata object level. For example, if the metadata
   * object is a table, the write lock of the table level will be held. The concrete implementation
   * may perform partial drops, meaning that the underlying storage system may not support
   * transactional delete.
   *
   * @param metalake the name of the metalake
   * @param partitionStatisticsToDrop a map where the key is a {@link MetadataObject} and the value
   *     is a list of {@link PartitionStatisticsDrop}
   * @return the number of statistics dropped, which may be less than the size of the input list if
   *     some statistics do not exist or cannot be dropped.
   */
  int dropStatistics(String metalake, List<MetadataObjectStatisticsDrop> partitionStatisticsToDrop)
      throws IOException;

  /**
   * Updates statistics for a given metadata object. If the statistic exists, it will be updated; If
   * the statistic doesn't exist, it will be created. Locking guarantee: The upper layer will
   * acquire a write lock at the metadata object level. For example, if the metadata object is a
   * table, the write lock of the table level will be held. The concrete implementation may perform
   * partial updates, meaning that the underlying storage system may not support transactional
   * update.
   *
   * @param metalake the name of the metalake
   * @param statisticsToUpdate a list of {@link MetadataObjectStatisticsUpdate} objects, each
   *     containing the metadata object and its associated statistics updates.
   */
  void updateStatistics(String metalake, List<MetadataObjectStatisticsUpdate> statisticsToUpdate)
      throws IOException;
}
