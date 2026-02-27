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
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.JobMetricWriteRequest;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricRecord;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricRecordImpl;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricsRepository;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.TableMetricWriteRequest;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc.GenericJdbcMetricsRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TestGravitinoMetricsUpdater {

  @Test
  void testUpdateTableMetricsWithoutInitializeFailsFast() {
    GravitinoMetricsUpdater updater = new GravitinoMetricsUpdater();
    IllegalStateException exception =
        Assertions.assertThrows(
            IllegalStateException.class, () -> updater.updateTableMetrics(List.of()));
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
  void testUpdateTableMetricsPassThroughRequests() throws Exception {
    GravitinoMetricsUpdater updater = new GravitinoMetricsUpdater();
    MetricsRepository repository = Mockito.mock(MetricsRepository.class);
    setMetricsRepository(updater, repository);
    NameIdentifier tableId = NameIdentifier.of("catalog", "db", "table");
    List<TableMetricWriteRequest> inputRequests =
        List.of(
            new TableMetricWriteRequest(
                tableId, "row_count", Optional.empty(), new MetricRecordImpl(100L, "10")),
            new TableMetricWriteRequest(
                tableId,
                "file_count",
                Optional.of("dt=2026-01-01"),
                new MetricRecordImpl(101L, "3")));

    updater.updateTableMetrics(inputRequests);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<TableMetricWriteRequest>> requestsCaptor =
        ArgumentCaptor.forClass(List.class);

    Mockito.verify(repository, Mockito.times(1)).storeTableMetrics(requestsCaptor.capture());

    List<TableMetricWriteRequest> requests = requestsCaptor.getValue();
    Assertions.assertEquals(2, requests.size());
    Assertions.assertEquals(tableId, requests.get(0).nameIdentifier());
    Assertions.assertEquals(tableId, requests.get(1).nameIdentifier());
    Assertions.assertEquals(Optional.empty(), requests.get(0).partition());
    Assertions.assertEquals(Optional.of("dt=2026-01-01"), requests.get(1).partition());

    List<MetricRecord> records = requests.stream().map(TableMetricWriteRequest::metric).toList();
    Assertions.assertEquals(100L, records.get(0).getTimestamp());
    Assertions.assertEquals(101L, records.get(1).getTimestamp());

    Assertions.assertEquals("10", records.get(0).getValue());
    Assertions.assertEquals("3", records.get(1).getValue());
  }

  @Test
  void testUpdateJobMetricsPassThroughRequests() throws Exception {
    GravitinoMetricsUpdater updater = new GravitinoMetricsUpdater();
    MetricsRepository repository = Mockito.mock(MetricsRepository.class);
    setMetricsRepository(updater, repository);
    NameIdentifier jobId = NameIdentifier.of("catalog", "db", "job");
    List<JobMetricWriteRequest> inputRequests =
        List.of(
            new JobMetricWriteRequest(jobId, "duration", new MetricRecordImpl(200L, "20")),
            new JobMetricWriteRequest(jobId, "planning", new MetricRecordImpl(201L, "1.5")));

    updater.updateJobMetrics(inputRequests);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<JobMetricWriteRequest>> requestsCaptor =
        ArgumentCaptor.forClass(List.class);
    Mockito.verify(repository, Mockito.times(1)).storeJobMetrics(requestsCaptor.capture());

    List<JobMetricWriteRequest> requests = requestsCaptor.getValue();
    Assertions.assertEquals(jobId, requests.get(0).nameIdentifier());
    Assertions.assertEquals(jobId, requests.get(1).nameIdentifier());
    List<MetricRecord> records = requests.stream().map(JobMetricWriteRequest::metric).toList();
    Assertions.assertEquals(200L, records.get(0).getTimestamp());
    Assertions.assertEquals(201L, records.get(1).getTimestamp());
    Assertions.assertEquals("20", records.get(0).getValue());
    Assertions.assertEquals("1.5", records.get(1).getValue());
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
    String jdbcUrl = "jdbc:h2:mem:test_metrics_updater_repo_type;DB_CLOSE_DELAY=-1";
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
}
