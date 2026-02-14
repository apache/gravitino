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

package org.apache.gravitino.maintenance.optimizer.monitor.metrics;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionMetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.StatisticValueUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.H2MetricsStorage;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricRecordImpl;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestGravitinoMetricsProvider {

  @TempDir Path tempDir;

  @Test
  public void testReadTableJobAndPartitionMetrics() throws Exception {
    Path metricsPath = tempDir.resolve("metrics-test");
    Map<String, String> configs =
        Map.of(
            OptimizerConfig.OPTIMIZER_PREFIX
                + "h2-metrics."
                + H2MetricsStorage.H2MetricsStorageConfig.H2_METRICS_STORAGE_PATH,
            metricsPath.toString());
    OptimizerEnv optimizerEnv = new OptimizerEnv(new OptimizerConfig(configs));

    NameIdentifier table = NameIdentifier.parse("catalog.db.table1");
    NameIdentifier job = NameIdentifier.parse("job1");
    PartitionPath partitionPath =
        PartitionPath.of(List.of(new PartitionEntryImpl("dt", "2026-02-14")));

    H2MetricsStorage storage = new H2MetricsStorage();
    storage.initialize(configs);
    storage.storeTableMetric(
        table,
        "row_count",
        Optional.empty(),
        new MetricRecordImpl(100L, StatisticValueUtils.toString(StatisticValues.longValue(10L))));
    storage.storeTableMetric(
        table,
        "row_count",
        Optional.of(PartitionUtils.encodePartitionPath(partitionPath)),
        new MetricRecordImpl(101L, StatisticValueUtils.toString(StatisticValues.longValue(11L))));
    storage.storeJobMetric(
        job,
        "duration",
        new MetricRecordImpl(102L, StatisticValueUtils.toString(StatisticValues.longValue(99L))));
    storage.close();

    GravitinoMetricsProvider provider = new GravitinoMetricsProvider();
    provider.initialize(optimizerEnv);
    try {
      Map<String, List<MetricSample>> tableMetrics = provider.tableMetrics(table, 0L, 200L);
      Assertions.assertEquals(1, tableMetrics.get("row_count").size());
      Assertions.assertEquals(
          10L, tableMetrics.get("row_count").get(0).statistic().value().value());

      Map<String, List<MetricSample>> partitionMetrics =
          provider.partitionMetrics(table, partitionPath, 0L, 200L);
      Assertions.assertEquals(1, partitionMetrics.get("row_count").size());
      Assertions.assertTrue(
          partitionMetrics.get("row_count").get(0) instanceof PartitionMetricSample);

      Map<String, List<MetricSample>> jobMetrics = provider.jobMetrics(job, 0L, 200L);
      Assertions.assertEquals(1, jobMetrics.get("duration").size());
      Assertions.assertEquals(99L, jobMetrics.get("duration").get(0).statistic().value().value());
    } finally {
      provider.close();
    }
  }
}
