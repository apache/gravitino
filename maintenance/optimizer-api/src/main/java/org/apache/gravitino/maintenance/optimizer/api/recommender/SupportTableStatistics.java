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

package org.apache.gravitino.maintenance.optimizer.api.recommender;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;

/**
 * Provides table and partition-level statistics to {@link StrategyHandler} implementations. The
 * recommender will only call these methods when the handler declares {@code TABLE_STATISTICS} or
 * {@code PARTITION_STATISTICS} in {@link StrategyHandler#dataRequirements()}.
 */
@DeveloperApi
public interface SupportTableStatistics extends StatisticsProvider {

  /**
   * Retrieve table-level statistics.
   *
   * @param tableIdentifier catalog/schema/table identifier (must be three levels)
   * @return list of statistics; empty when none are available
   * @throws NoSuchTableException if the table does not exist
   */
  List<StatisticEntry<?>> tableStatistics(NameIdentifier tableIdentifier)
      throws NoSuchTableException;

  /**
   * Retrieve partition-level statistics.
   *
   * @param tableIdentifier catalog/schema/table identifier (must be three levels)
   * @return map keyed by partition path with partition-level statistics; empty when none are
   *     available or the table is unpartitioned
   * @throws NoSuchTableException if the table does not exist
   */
  Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics(NameIdentifier tableIdentifier)
      throws NoSuchTableException;
}
