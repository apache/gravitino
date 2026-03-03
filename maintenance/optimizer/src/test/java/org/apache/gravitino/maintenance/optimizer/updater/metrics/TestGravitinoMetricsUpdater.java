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

package org.apache.gravitino.maintenance.optimizer.updater.metrics;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.DataScope;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricsRepository;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc.GenericJdbcMetricsRepository;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TestGravitinoMetricsUpdater {

  @Test
  void testUpdateTableAndPartitionMetricsWithoutInitializeFailsFast() {
    GravitinoMetricsUpdater updater = new GravitinoMetricsUpdater();
    IllegalStateException exception =
        Assertions.assertThrows(
            IllegalStateException.class, () -> updater.updateTableAndPartitionMetrics(List.of()));
    Assertions.assertTrue(exception.getMessage().contains("has not been initialized"));
  }

  @Test
  void testUpdateJobMetricsWithoutInitializeFailsFast() {
    GravitinoMetricsUpdater updater = new GravitinoMetricsUpdater();
    IllegalStateException exception =
        Assertions.assertThrows(
            IllegalStateException.class, () -> updater.updateJobMetrics(List.of()));
    Assertions.assertTrue(exception.getMessage().contains("has not been initialized"));
  }

  @Test
  void testUpdateTableAndPartitionMetricsPassThroughRequests() throws Exception {
    GravitinoMetricsUpdater updater = new GravitinoMetricsUpdater();
    MetricsRepository repository = Mockito.mock(MetricsRepository.class);
    setMetricsRepository(updater, repository);
    NameIdentifier tableId = NameIdentifier.of("catalog", "db", "table");
    List<MetricPoint> inputMetrics =
        List.of(
            MetricPoint.forTable(tableId, "row_count", StatisticValues.longValue(10L), 100L),
            MetricPoint.forPartition(
                tableId,
                parsePartitionPath("dt=2026-03-03"),
                "row_count",
                StatisticValues.longValue(9L),
                110L));

    updater.updateTableAndPartitionMetrics(inputMetrics);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<MetricPoint>> requestsCaptor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(repository, Mockito.times(1))
        .storeTableAndPartitionMetrics(requestsCaptor.capture());

    List<MetricPoint> metrics = requestsCaptor.getValue();
    Assertions.assertEquals(2, metrics.size());
    Assertions.assertEquals(tableId, metrics.get(0).identifier());
    Assertions.assertEquals(DataScope.Type.TABLE, metrics.get(0).scope());
    Assertions.assertEquals(100L, metrics.get(0).timestampSeconds());
    Assertions.assertEquals(10L, ((Number) metrics.get(0).value().value()).longValue());
    Assertions.assertEquals(tableId, metrics.get(1).identifier());
    Assertions.assertEquals(DataScope.Type.PARTITION, metrics.get(1).scope());
    Assertions.assertEquals(110L, metrics.get(1).timestampSeconds());
    Assertions.assertEquals(9L, ((Number) metrics.get(1).value().value()).longValue());
  }

  @Test
  void testUpdateJobMetricsPassThroughRequests() throws Exception {
    GravitinoMetricsUpdater updater = new GravitinoMetricsUpdater();
    MetricsRepository repository = Mockito.mock(MetricsRepository.class);
    setMetricsRepository(updater, repository);
    NameIdentifier jobId = NameIdentifier.of("catalog", "db", "job");
    List<MetricPoint> inputMetrics =
        List.of(MetricPoint.forJob(jobId, "duration", StatisticValues.longValue(20L), 200L));

    updater.updateJobMetrics(inputMetrics);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<MetricPoint>> requestsCaptor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(repository, Mockito.times(1)).storeJobMetrics(requestsCaptor.capture());

    List<MetricPoint> metrics = requestsCaptor.getValue();
    Assertions.assertEquals(1, metrics.size());
    Assertions.assertEquals(jobId, metrics.get(0).identifier());
    Assertions.assertEquals(DataScope.Type.JOB, metrics.get(0).scope());
    Assertions.assertEquals(200L, metrics.get(0).timestampSeconds());
    Assertions.assertEquals(20L, ((Number) metrics.get(0).value().value()).longValue());
  }

  @Test
  void testUpdateTableAndPartitionMetricsRejectsJobScope() throws Exception {
    GravitinoMetricsUpdater updater = new GravitinoMetricsUpdater();
    MetricsRepository repository = Mockito.mock(MetricsRepository.class);
    setMetricsRepository(updater, repository);
    NameIdentifier jobId = NameIdentifier.of("catalog", "db", "job");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            updater.updateTableAndPartitionMetrics(
                List.of(
                    MetricPoint.forJob(jobId, "duration", StatisticValues.longValue(20L), 200L))));
  }

  @Test
  void testUpdateJobMetricsRejectsTableScope() throws Exception {
    GravitinoMetricsUpdater updater = new GravitinoMetricsUpdater();
    MetricsRepository repository = Mockito.mock(MetricsRepository.class);
    setMetricsRepository(updater, repository);
    NameIdentifier tableId = NameIdentifier.of("catalog", "db", "table");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            updater.updateJobMetrics(
                List.of(
                    MetricPoint.forTable(
                        tableId, "row_count", StatisticValues.longValue(10L), 100L))));
  }

  @Test
  void testCloseDelegatesToRepository() throws Exception {
    GravitinoMetricsUpdater updater = new GravitinoMetricsUpdater();
    MetricsRepository repository = Mockito.mock(MetricsRepository.class);
    setMetricsRepository(updater, repository);

    updater.close();

    Mockito.verify(repository).close();
  }

  @Test
  void testInitializeDefaultUsesGenericJdbcRepository() throws Exception {
    GravitinoMetricsUpdater updater = new GravitinoMetricsUpdater();
    String storagePath = "data/test-metrics-updater-default-" + System.nanoTime() + ".db";
    String jdbcUrl = "jdbc:h2:file:./" + storagePath + ";DB_CLOSE_DELAY=-1;MODE=MYSQL";
    updater.initialize(
        new OptimizerEnv(
            new OptimizerConfig(
                Map.of(
                    OptimizerConfig.OPTIMIZER_PREFIX + "jdbcMetrics." + "jdbcUrl",
                    jdbcUrl,
                    OptimizerConfig.OPTIMIZER_PREFIX + "jdbcMetrics." + "jdbcUser",
                    "sa",
                    OptimizerConfig.OPTIMIZER_PREFIX + "jdbcMetrics." + "jdbcPassword",
                    "",
                    OptimizerConfig.OPTIMIZER_PREFIX + "jdbcMetrics." + "jdbcDriver",
                    "org.h2.Driver",
                    OptimizerConfig.OPTIMIZER_PREFIX + "jdbcMetrics." + "testOnBorrow",
                    "false"))));
    MetricsRepository repository = getMetricsRepository(updater);
    Assertions.assertInstanceOf(GenericJdbcMetricsRepository.class, repository);
    updater.close();
  }

  @Test
  void testInitializeWithJdbcConfigStillUsesGenericJdbcRepository() throws Exception {
    GravitinoMetricsUpdater updater = new GravitinoMetricsUpdater();
    String jdbcUrl = "jdbc:h2:mem:test_metrics_updater_repo_type;DB_CLOSE_DELAY=-1;MODE=MYSQL";
    OptimizerConfig config =
        new OptimizerConfig(
            Map.of(
                "gravitino.optimizer.jdbcMetrics.jdbcUrl",
                jdbcUrl,
                "gravitino.optimizer.jdbcMetrics.jdbcUser",
                "sa",
                "gravitino.optimizer.jdbcMetrics.jdbcPassword",
                ""));
    updater.initialize(new OptimizerEnv(config));
    MetricsRepository repository = getMetricsRepository(updater);
    Assertions.assertInstanceOf(GenericJdbcMetricsRepository.class, repository);
    updater.close();
  }

  private void setMetricsRepository(GravitinoMetricsUpdater updater, MetricsRepository repository)
      throws ReflectiveOperationException {
    Field field = GravitinoMetricsUpdater.class.getDeclaredField("metricsStorage");
    field.setAccessible(true);
    field.set(updater, repository);
  }

  private MetricsRepository getMetricsRepository(GravitinoMetricsUpdater updater)
      throws ReflectiveOperationException {
    Field field = GravitinoMetricsUpdater.class.getDeclaredField("metricsStorage");
    field.setAccessible(true);
    return (MetricsRepository) field.get(updater);
  }

  private PartitionPath parsePartitionPath(String partition) {
    String[] entries = partition.split("/");
    List<PartitionEntry> partitionEntries = new java.util.ArrayList<>(entries.length);
    for (String entry : entries) {
      String[] kv = entry.split("=", 2);
      partitionEntries.add(new PartitionEntryImpl(kv[0], kv[1]));
    }
    return PartitionPath.of(partitionEntries);
  }
}
