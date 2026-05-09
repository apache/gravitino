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

package org.apache.gravitino.maintenance.optimizer.integration.test;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.TableAndPartitionStatistics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateTableMetrics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateTableStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.stats.StatisticValues;

public class DummyTableStatisticsComputer
    implements SupportsCalculateTableStatistics, SupportsCalculateTableMetrics {

  public static final String DUMMY_TABLE_STAT = "dummy-table-stat";
  public static final String TABLE_STAT_NAME = "custom-dummy-table-stat-name";

  @Override
  public String name() {
    return DUMMY_TABLE_STAT;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {}

  @Override
  public TableAndPartitionStatistics calculateTableStatistics(NameIdentifier tableIdentifier) {
    List<StatisticEntry<?>> tableStatistics =
        List.of(new StatisticEntryImpl<>(TABLE_STAT_NAME, StatisticValues.longValue(1L)));
    Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics =
        Map.of(
            PartitionPath.of(getPartitionEntries()),
            List.of(new StatisticEntryImpl<>(TABLE_STAT_NAME, StatisticValues.longValue(2L))));
    return new TableAndPartitionStatistics(tableStatistics, partitionStatistics);
  }

  @Override
  public List<MetricPoint> calculateTableMetrics(NameIdentifier tableIdentifier) {
    long timestampSeconds = System.currentTimeMillis() / 1000;
    PartitionPath partitionPath = PartitionPath.of(getPartitionEntries());
    return List.of(
        MetricPoint.forTable(
            tableIdentifier, TABLE_STAT_NAME, StatisticValues.longValue(1L), timestampSeconds),
        MetricPoint.forPartition(
            tableIdentifier,
            partitionPath,
            TABLE_STAT_NAME,
            StatisticValues.longValue(2L),
            timestampSeconds));
  }

  @VisibleForTesting
  public static List<PartitionEntry> getPartitionEntries() {
    return List.of(new PartitionEntryImpl("p1", "v1"));
  }
}
