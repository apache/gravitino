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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.DataScope;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricsRepository;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc.GenericJdbcMetricsRepository;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestGravitinoMetricsProvider {

  @TempDir Path tempDir;

  @Test
  public void testReadTableJobAndPartitionMetrics() throws Exception {
    Path metricsPath = tempDir.resolve("metrics-test");
    String jdbcUrl = "jdbc:h2:file:" + metricsPath + ";DB_CLOSE_DELAY=-1;MODE=MYSQL";
    Map<String, String> configs =
        Map.of(
            OptimizerConfig.OPTIMIZER_PREFIX
                + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX
                + GenericJdbcMetricsRepository.JDBC_URL,
            jdbcUrl,
            OptimizerConfig.OPTIMIZER_PREFIX
                + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX
                + GenericJdbcMetricsRepository.JDBC_USER,
            "sa",
            OptimizerConfig.OPTIMIZER_PREFIX
                + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX
                + GenericJdbcMetricsRepository.JDBC_PASSWORD,
            "");
    OptimizerEnv optimizerEnv = new OptimizerEnv(new OptimizerConfig(configs));

    NameIdentifier table = NameIdentifier.parse("catalog.db.table1");
    NameIdentifier job = NameIdentifier.parse("job1");
    PartitionPath partitionPath =
        PartitionPath.of(List.of(new PartitionEntryImpl("dt", "2026-02-14")));

    try (MetricsRepository metricsRepository = new GenericJdbcMetricsRepository()) {
      metricsRepository.initialize(configs);
      metricsRepository.storeTableAndPartitionMetrics(
          List.of(
              MetricPoint.forTable(table, "row_count", StatisticValues.longValue(10L), 100L),
              MetricPoint.forPartition(
                  table, partitionPath, "row_count", StatisticValues.longValue(11L), 101L)));
      metricsRepository.storeJobMetrics(
          List.of(MetricPoint.forJob(job, "duration", StatisticValues.longValue(99L), 102L)));
    }

    GravitinoMetricsProvider provider = new GravitinoMetricsProvider();
    provider.initialize(optimizerEnv);
    try {
      List<MetricPoint> tableMetrics = provider.tableMetrics(table, 0L, 200L);
      Assertions.assertEquals(1, tableMetrics.size());
      Assertions.assertEquals(10L, tableMetrics.get(0).value().value());

      List<MetricPoint> partitionMetrics =
          provider.partitionMetrics(table, partitionPath, 0L, 200L);
      Assertions.assertEquals(1, partitionMetrics.size());
      Assertions.assertEquals(DataScope.Type.PARTITION, partitionMetrics.get(0).scope());

      List<MetricPoint> jobMetrics = provider.jobMetrics(job, 0L, 200L);
      Assertions.assertEquals(1, jobMetrics.size());
      Assertions.assertEquals(99L, jobMetrics.get(0).value().value());
    } finally {
      provider.close();
    }
  }
}
