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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.recommender.statistics.GravitinoStatisticsProvider;
import org.apache.gravitino.maintenance.optimizer.updater.statistics.GravitinoStatisticsUpdater;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GravitinoStatisticsIT extends GravitinoOptimizerEnvIT {

  private static final String TEST_TABLE = "test_stats_table";
  private static final String TEST_PARTITION_TABLE = "test_stats_partition_table";
  private static final String STATISTICS_PREFIX = "custom-";
  private static final String DATAFILE_SIZE_MSE = STATISTICS_PREFIX + "datafile_size_mse";

  private GravitinoStatisticsUpdater statisticsUpdater;
  private GravitinoStatisticsProvider statisticsProvider;

  @BeforeAll
  void init() {
    this.statisticsUpdater = new GravitinoStatisticsUpdater();
    statisticsUpdater.initialize(optimizerEnv);
    this.statisticsProvider = new GravitinoStatisticsProvider();
    statisticsProvider.initialize(optimizerEnv);
    createTable(TEST_TABLE);
    createPartitionTable(TEST_PARTITION_TABLE);
  }

  @AfterAll
  void closeResources() throws Exception {
    if (statisticsProvider != null) {
      statisticsProvider.close();
    }
    if (statisticsUpdater != null) {
      statisticsUpdater.close();
    }
  }

  @Test
  void testTableStatisticsUpdaterAndProvider() {
    statisticsUpdater.updateTableStatistics(
        getTableIdentifier(TEST_TABLE),
        Arrays.asList(
            new StatisticEntryImpl<>(
                STATISTICS_PREFIX + "row_count", StatisticValues.longValue(1000)),
            new StatisticEntryImpl<>(
                STATISTICS_PREFIX + "size_in_bytes", StatisticValues.longValue(1000000)),
            new StatisticEntryImpl<>(DATAFILE_SIZE_MSE, StatisticValues.doubleValue(10000.1))));

    List<StatisticEntry<?>> stats =
        statisticsProvider.tableStatistics(getTableIdentifier(TEST_TABLE));
    Assertions.assertEquals(3, stats.size());
    stats.forEach(
        stat -> {
          if (stat.name().equals(STATISTICS_PREFIX + "row_count")) {
            Assertions.assertEquals(1000L, ((Number) stat.value().value()).longValue());
          } else if (stat.name().equals(STATISTICS_PREFIX + "size_in_bytes")) {
            Assertions.assertEquals(1000000L, ((Number) stat.value().value()).longValue());
          } else if (stat.name().equals(DATAFILE_SIZE_MSE)) {
            Assertions.assertEquals(10000.1, ((Number) stat.value().value()).doubleValue());
          } else {
            Assertions.fail("Unexpected statistic name: " + stat.name());
          }
        });
  }

  @Test
  void testTablePartitionStatisticsUpdaterAndProvider() {
    statisticsUpdater.updateTableStatistics(
        getTableIdentifier(TEST_PARTITION_TABLE),
        Arrays.asList(
            new StatisticEntryImpl<>(
                STATISTICS_PREFIX + "size_in_bytes", StatisticValues.longValue(1000000))));
    statisticsUpdater.updatePartitionStatistics(
        getTableIdentifier(TEST_PARTITION_TABLE),
        Map.of(
            PartitionPath.of(
                Arrays.asList(
                    new PartitionEntryImpl("col1", "1"), new PartitionEntryImpl("col2", "2"))),
            List.of(
                new StatisticEntryImpl<>(
                    STATISTICS_PREFIX + "partition_row_count", StatisticValues.longValue(500)),
                new StatisticEntryImpl<>(
                    STATISTICS_PREFIX + "partition_size_in_bytes",
                    StatisticValues.longValue(500000)))));

    Map<PartitionPath, List<StatisticEntry<?>>> stats =
        statisticsProvider.partitionStatistics(getTableIdentifier(TEST_PARTITION_TABLE));
    Assertions.assertEquals(1, stats.size());
    List<StatisticEntry<?>> partitionStats =
        stats.values().stream().findFirst().orElseThrow(IllegalStateException::new);
    Assertions.assertEquals(2, partitionStats.size());
    partitionStats.forEach(
        stat -> {
          if (stat.name().equals(STATISTICS_PREFIX + "partition_row_count")) {
            Assertions.assertEquals(500L, ((Number) stat.value().value()).longValue());
          } else if (stat.name().equals(STATISTICS_PREFIX + "partition_size_in_bytes")) {
            Assertions.assertEquals(500000L, ((Number) stat.value().value()).longValue());
          } else {
            Assertions.fail("Unexpected statistic name: " + stat.name());
          }
        });
  }
}
