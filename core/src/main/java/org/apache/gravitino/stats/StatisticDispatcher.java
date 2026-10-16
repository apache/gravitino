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

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.exceptions.UnmodifiableStatisticException;

/** The StatisticDispatcher interface provides methods to manage statistics for metadata objects */
public interface StatisticDispatcher extends Closeable {

  /**
   * List statistics for a given metadata object in a metalake.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object
   * @return List of statistics
   */
  List<Statistic> listStatistics(String metalake, MetadataObject metadataObject);

  /**
   * Update statistics for a given metadata object in a metalake.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object
   * @param statistics the statistics to update
   */
  void updateStatistics(
      String metalake, MetadataObject metadataObject, Map<String, StatisticValue<?>> statistics);

  /**
   * Drop statistics for a given metadata object in a metalake.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object
   * @param statistics the statistics to drop
   * @return true if statistics were dropped, false otherwise
   * @throws UnmodifiableStatisticException if any of the statistics cannot be modified
   */
  boolean dropStatistics(String metalake, MetadataObject metadataObject, List<String> statistics);

  /**
   * Drop partition statistics for a given metadata object in a metalake.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object
   * @param partitionStatistics the partition statistics to drop
   * @return true if partition statistics were dropped, false otherwise
   */
  boolean dropPartitionStatistics(
      String metalake,
      MetadataObject metadataObject,
      List<PartitionStatisticsDrop> partitionStatistics);

  /**
   * Update partition statistics for a given metadata object in a metalake.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object
   * @param partitionStatistics the partition statistics to update
   */
  void updatePartitionStatistics(
      String metalake,
      MetadataObject metadataObject,
      List<PartitionStatisticsUpdate> partitionStatistics);

  /**
   * List partition statistics for a given metadata object in a metalake within a specified range.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object
   * @param range the partition range
   * @return List of partition statistics
   */
  List<PartitionStatistics> listPartitionStatistics(
      String metalake, MetadataObject metadataObject, PartitionRange range);
}
