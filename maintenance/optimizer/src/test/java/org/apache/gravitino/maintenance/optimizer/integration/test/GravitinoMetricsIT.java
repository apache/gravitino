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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.monitor.metrics.GravitinoMetricsProvider;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.GravitinoMetricsUpdater;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc.GenericJdbcMetricsRepository;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GravitinoMetricsIT {

  private GravitinoMetricsUpdater updater;
  private GravitinoMetricsProvider provider;

  @BeforeAll
  public void setUp() {
    OptimizerEnv optimizerEnv = createOptimizerEnv();
    updater = new GravitinoMetricsUpdater();
    updater.initialize(optimizerEnv);
    provider = new GravitinoMetricsProvider();
    provider.initialize(optimizerEnv);
  }

  @AfterAll
  public void tearDown() throws Exception {
    if (updater != null) {
      updater.close();
    }
    if (provider != null) {
      provider.close();
    }
  }

  @Test
  void testTableMetrics() {
    NameIdentifier tableIdentifier = NameIdentifier.of("catalog", "schema", "table");

    PartitionPath partition1 =
        PartitionPath.of(
            Arrays.asList(new PartitionEntryImpl("p1", "v1"), new PartitionEntryImpl("p2", "v2")));
    PartitionPath partition2 =
        PartitionPath.of(
            Arrays.asList(new PartitionEntryImpl("p1", "v11"), new PartitionEntryImpl("p2", "v2")));

    updater.updateTableAndPartitionMetrics(
        Arrays.asList(
            MetricPoint.forTable(tableIdentifier, "a", StatisticValues.longValue(10), 1000),
            MetricPoint.forTable(tableIdentifier, "b", StatisticValues.longValue(1000), 1000),
            MetricPoint.forPartition(
                tableIdentifier, partition1, "b", StatisticValues.longValue(1003), 1000),
            MetricPoint.forPartition(
                tableIdentifier, partition2, "b", StatisticValues.longValue(1004), 1000),
            MetricPoint.forTable(tableIdentifier, "a", StatisticValues.longValue(100L), 1001)));

    List<MetricPoint> tableMetrics = provider.tableMetrics(tableIdentifier, 1000, 1002);
    Map<String, List<MetricPoint>> tableMetricsByName = metricsByName(tableMetrics);

    Assertions.assertEquals(2, tableMetricsByName.size());

    List<MetricPoint> aMetrics = sortByTimestamp(tableMetricsByName.get("a"));
    Assertions.assertEquals(2, aMetrics.size());
    Assertions.assertEquals(10L, ((Number) aMetrics.get(0).value().value()).longValue());
    Assertions.assertEquals(1000L, aMetrics.get(0).timestampSeconds());
    Assertions.assertEquals(100L, ((Number) aMetrics.get(1).value().value()).longValue());
    Assertions.assertEquals(1001L, aMetrics.get(1).timestampSeconds());

    List<MetricPoint> bMetrics = tableMetricsByName.get("b");
    Assertions.assertEquals(1, bMetrics.size());
    Assertions.assertEquals(1000L, ((Number) bMetrics.get(0).value().value()).longValue());
    Assertions.assertEquals(1000L, bMetrics.get(0).timestampSeconds());

    List<MetricPoint> partitionMetrics1 =
        provider.partitionMetrics(tableIdentifier, partition1, 1000, 1002);
    Assertions.assertEquals(1, partitionMetrics1.size());
    MetricPoint metric1 = partitionMetrics1.get(0);
    Assertions.assertEquals("b", metric1.metricName());
    Assertions.assertEquals(
        partition1, metric1.partitionPath().orElseThrow(IllegalStateException::new));
    Assertions.assertEquals(1003L, ((Number) metric1.value().value()).longValue());
    Assertions.assertEquals(1000L, metric1.timestampSeconds());

    List<MetricPoint> partitionMetrics2 =
        provider.partitionMetrics(tableIdentifier, partition2, 1000, 1002);
    Assertions.assertEquals(1, partitionMetrics2.size());
    MetricPoint metric2 = partitionMetrics2.get(0);
    Assertions.assertEquals("b", metric2.metricName());
    Assertions.assertEquals(
        partition2, metric2.partitionPath().orElseThrow(IllegalStateException::new));
    Assertions.assertEquals(1004L, ((Number) metric2.value().value()).longValue());
    Assertions.assertEquals(1000L, metric2.timestampSeconds());
  }

  @Test
  void testJobMetrics() {
    NameIdentifier jobIdentifier = NameIdentifier.of("job1");

    updater.updateJobMetrics(
        Arrays.asList(
            MetricPoint.forJob(jobIdentifier, "x", StatisticValues.longValue(20), 2000),
            MetricPoint.forJob(jobIdentifier, "y", StatisticValues.longValue(2000), 2000),
            MetricPoint.forJob(jobIdentifier, "x", StatisticValues.longValue(200L), 2001)));

    List<MetricPoint> jobMetrics = provider.jobMetrics(jobIdentifier, 2000, 2002);
    Map<String, List<MetricPoint>> jobMetricsByName = metricsByName(jobMetrics);

    Assertions.assertEquals(2, jobMetricsByName.size());

    List<MetricPoint> xMetrics = sortByTimestamp(jobMetricsByName.get("x"));
    Assertions.assertEquals(2, xMetrics.size());
    Assertions.assertEquals(20L, ((Number) xMetrics.get(0).value().value()).longValue());
    Assertions.assertEquals(2000L, xMetrics.get(0).timestampSeconds());
    Assertions.assertEquals(200L, ((Number) xMetrics.get(1).value().value()).longValue());
    Assertions.assertEquals(2001L, xMetrics.get(1).timestampSeconds());

    List<MetricPoint> yMetrics = jobMetricsByName.get("y");
    Assertions.assertEquals(1, yMetrics.size());
    Assertions.assertEquals(2000L, ((Number) yMetrics.get(0).value().value()).longValue());
    Assertions.assertEquals(2000L, yMetrics.get(0).timestampSeconds());
  }

  private static Map<String, List<MetricPoint>> metricsByName(List<MetricPoint> points) {
    return points.stream().collect(Collectors.groupingBy(MetricPoint::metricName));
  }

  private static List<MetricPoint> sortByTimestamp(List<MetricPoint> points) {
    return points.stream().sorted(Comparator.comparingLong(MetricPoint::timestampSeconds)).toList();
  }

  private OptimizerEnv createOptimizerEnv() {
    String jdbcUrl =
        String.format(
            "jdbc:h2:file:/tmp/gravitino-metrics-it-%d;DB_CLOSE_DELAY=-1;MODE=MYSQL;AUTO_SERVER=TRUE",
            System.nanoTime());

    Map<String, String> config =
        Map.of(
            OptimizerConfig.OPTIMIZER_PREFIX
                + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX
                + GenericJdbcMetricsRepository.JDBC_URL,
            jdbcUrl,
            OptimizerConfig.OPTIMIZER_PREFIX
                + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX
                + GenericJdbcMetricsRepository.JDBC_DRIVER,
            "org.h2.Driver");
    return new OptimizerEnv(new OptimizerConfig(config));
  }
}
