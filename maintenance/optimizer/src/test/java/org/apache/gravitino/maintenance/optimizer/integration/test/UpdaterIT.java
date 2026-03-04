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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.monitor.metrics.GravitinoMetricsProvider;
import org.apache.gravitino.maintenance.optimizer.recommender.statistics.GravitinoStatisticsProvider;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.maintenance.optimizer.updater.UpdateType;
import org.apache.gravitino.maintenance.optimizer.updater.Updater;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class UpdaterIT extends GravitinoOptimizerEnvIT {

  private Updater updater;
  private GravitinoStatisticsProvider statisticsProvider;
  private GravitinoMetricsProvider metricsProvider;

  @Override
  protected Map<String, String> getSpecifyConfigs() {
    return Map.of();
  }

  @BeforeAll
  void init() {
    this.updater = new Updater(optimizerEnv);
    this.statisticsProvider = new GravitinoStatisticsProvider();
    statisticsProvider.initialize(optimizerEnv);
    this.metricsProvider = new GravitinoMetricsProvider();
    metricsProvider.initialize(optimizerEnv);
  }

  @Test
  void testUpdateTableStatistics() {
    String tableName = "update-stats";
    createTable(tableName);
    NameIdentifier tableIdentifier = getTableIdentifier(tableName);
    updater.update(
        DummyTableStatisticsComputer.DUMMY_TABLE_STAT,
        Arrays.asList(tableIdentifier),
        UpdateType.STATISTICS);

    List<StatisticEntry<?>> tableStats = statisticsProvider.tableStatistics(tableIdentifier);
    Assertions.assertEquals(1, tableStats.size());
    Assertions.assertEquals(DummyTableStatisticsComputer.TABLE_STAT_NAME, tableStats.get(0).name());
    Assertions.assertEquals(1L, ((Number) tableStats.get(0).value().value()).longValue());

    Map<PartitionPath, List<StatisticEntry<?>>> partitionStats =
        statisticsProvider.partitionStatistics(tableIdentifier);
    Assertions.assertEquals(1, partitionStats.size());
    List<StatisticEntry<?>> partitionEntries =
        partitionStats.values().stream().findFirst().orElseThrow(IllegalStateException::new);
    Assertions.assertEquals(1, partitionEntries.size());
    Assertions.assertEquals(
        DummyTableStatisticsComputer.TABLE_STAT_NAME, partitionEntries.get(0).name());
    Assertions.assertEquals(2L, ((Number) partitionEntries.get(0).value().value()).longValue());
    Assertions.assertEquals(
        PartitionUtils.encodePartitionPath(
            PartitionPath.of(DummyTableStatisticsComputer.getPartitionName())),
        PartitionUtils.encodePartitionPath(
            partitionStats.keySet().stream().findFirst().orElseThrow(IllegalStateException::new)));
  }

  @Test
  void testUpdateTableMetrics() {
    String tableName = "update-metrics";
    createTable(tableName);
    NameIdentifier tableIdentifier = getTableIdentifier(tableName);
    updater.update(
        DummyTableStatisticsComputer.DUMMY_TABLE_STAT,
        Arrays.asList(tableIdentifier),
        UpdateType.METRICS);

    List<MetricPoint> tableMetrics =
        metricsProvider.tableMetrics(tableIdentifier, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, tableMetrics.size());
    MetricPoint tableMetric = tableMetrics.get(0);
    Assertions.assertEquals(DummyTableStatisticsComputer.TABLE_STAT_NAME, tableMetric.metricName());
    long tableDiff = System.currentTimeMillis() / 1000 - tableMetric.timestampSeconds();
    Assertions.assertTrue(tableDiff >= 0 && tableDiff <= 10000);
    Assertions.assertEquals(1L, ((Number) tableMetric.value().value()).longValue());

    PartitionPath expectedPartition =
        PartitionPath.of(DummyTableStatisticsComputer.getPartitionName());
    List<MetricPoint> partitionMetrics =
        metricsProvider.partitionMetrics(tableIdentifier, expectedPartition, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, partitionMetrics.size());
    MetricPoint partitionMetric = partitionMetrics.get(0);
    Assertions.assertEquals(
        DummyTableStatisticsComputer.TABLE_STAT_NAME, partitionMetric.metricName());
    long partitionDiff = System.currentTimeMillis() / 1000 - partitionMetric.timestampSeconds();
    Assertions.assertTrue(partitionDiff >= 0 && partitionDiff <= 10000);
    Assertions.assertEquals(2L, ((Number) partitionMetric.value().value()).longValue());
    Assertions.assertEquals(
        PartitionUtils.encodePartitionPath(expectedPartition),
        PartitionUtils.encodePartitionPath(partitionMetric.partitionPath().orElseThrow()));
  }

  @Test
  void testUpdateJobMetrics() {
    String jobName = "update-job-metrics";
    NameIdentifier jobIdentifier = NameIdentifier.of(jobName);
    updater.update(
        DummyJobMetricsComputer.DUMMY_JOB_METRICS,
        Arrays.asList(jobIdentifier),
        UpdateType.METRICS);

    List<MetricPoint> jobMetrics = metricsProvider.jobMetrics(jobIdentifier, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, jobMetrics.size());
    MetricPoint jobMetric = jobMetrics.get(0);
    Assertions.assertEquals(DummyJobMetricsComputer.JOB_STAT_NAME, jobMetric.metricName());
    long diff = System.currentTimeMillis() / 1000 - jobMetric.timestampSeconds();
    Assertions.assertTrue(diff >= 0 && diff <= 10000);
    Assertions.assertEquals(1L, ((Number) jobMetric.value().value()).longValue());
  }
}
