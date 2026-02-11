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

package org.apache.gravitino.maintenance.optimizer.api.common;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;

/** Container for table-level statistics and partition-level statistics. */
@DeveloperApi
public class TableStatisticsBundle {
  private final List<StatisticEntry<?>> tableStatistics;
  private final Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics;

  public TableStatisticsBundle(
      List<StatisticEntry<?>> tableStatistics,
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics) {
    this.tableStatistics = tableStatistics != null ? List.copyOf(tableStatistics) : List.of();
    this.partitionStatistics =
        partitionStatistics != null ? Map.copyOf(partitionStatistics) : Map.of();
  }

  public List<StatisticEntry<?>> tableStatistics() {
    return tableStatistics;
  }

  public Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics() {
    return partitionStatistics;
  }
}
