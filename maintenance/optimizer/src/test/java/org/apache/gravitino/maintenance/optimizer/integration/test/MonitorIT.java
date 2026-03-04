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

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.DataScope;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.monitor.EvaluationResult;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.monitor.Monitor;
import org.apache.gravitino.maintenance.optimizer.monitor.callback.MonitorCallbackForTest;
import org.apache.gravitino.maintenance.optimizer.monitor.evaluator.MetricsEvaluatorForTest;
import org.apache.gravitino.maintenance.optimizer.monitor.job.TableJobRelationProviderForTest;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.GravitinoMetricsUpdater;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc.GenericJdbcMetricsRepository;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MonitorIT {

  private GravitinoMetricsUpdater updater;
  private Monitor monitor;

  @BeforeAll
  public void setUp() {
    OptimizerEnv env = createOptimizerEnv();
    this.updater = new GravitinoMetricsUpdater();
    updater.initialize(env);
    this.monitor = new Monitor(env);
    MonitorCallbackForTest.reset();
  }

  @Test
  void testTableMetrics() {
    long actionTime = 10;
    long rangeSeconds = 2;
    NameIdentifier tableIdentifier = NameIdentifier.of("db", "table");
    NameIdentifier job1 = TableJobRelationProviderForTest.JOB1;
    NameIdentifier job2 = TableJobRelationProviderForTest.JOB2;

    updater.updateTableAndPartitionMetrics(
        Arrays.asList(
            MetricPoint.forTable(tableIdentifier, "storage", StatisticValues.longValue(10), 8),
            MetricPoint.forTable(tableIdentifier, "s3_cost", StatisticValues.longValue(1000), 9),
            MetricPoint.forTable(tableIdentifier, "s3_cost", StatisticValues.longValue(1003), 10),
            MetricPoint.forTable(tableIdentifier, "storage", StatisticValues.longValue(100L), 11)));

    List<MetricPoint> jobMetrics =
        Arrays.asList(
            MetricPoint.forJob(job1, "job_runtime", StatisticValues.longValue(8), 8),
            MetricPoint.forJob(job1, "job_cost", StatisticValues.longValue(9), 9),
            MetricPoint.forJob(job1, "job_cost", StatisticValues.longValue(10), 10),
            MetricPoint.forJob(job1, "job_cost", StatisticValues.longValue(11), 100),
            MetricPoint.forJob(job1, "job_runtime", StatisticValues.longValue(12L), 11),
            MetricPoint.forJob(job2, "job_runtime", StatisticValues.longValue(8), 8),
            MetricPoint.forJob(job2, "job_cost", StatisticValues.longValue(9), 9),
            MetricPoint.forJob(job2, "job_cost", StatisticValues.longValue(10), 10),
            MetricPoint.forJob(job2, "job_cost", StatisticValues.longValue(11), 100),
            MetricPoint.forJob(job2, "job_runtime", StatisticValues.longValue(12L), 11));
    updater.updateJobMetrics(jobMetrics);

    List<EvaluationResult> results =
        monitor.evaluateMetrics(tableIdentifier, actionTime, rangeSeconds, Optional.empty());
    Assertions.assertEquals(3, results.size());

    EvaluationResult tableResult = findByScope(results, DataScope.Type.TABLE, tableIdentifier);

    List<MetricSample> storageBefore = sortedSamples(tableResult.beforeMetrics().get("storage"));
    Assertions.assertEquals(1, storageBefore.size());
    Assertions.assertEquals(8L, storageBefore.get(0).timestampSeconds());
    Assertions.assertEquals(10L, ((Number) storageBefore.get(0).value().value()).longValue());

    List<MetricSample> s3Before = sortedSamples(tableResult.beforeMetrics().get("s3_cost"));
    Assertions.assertEquals(1, s3Before.size());
    Assertions.assertEquals(9L, s3Before.get(0).timestampSeconds());
    Assertions.assertEquals(1000L, ((Number) s3Before.get(0).value().value()).longValue());

    List<MetricSample> storageAfter = sortedSamples(tableResult.afterMetrics().get("storage"));
    Assertions.assertEquals(1, storageAfter.size());
    Assertions.assertEquals(11L, storageAfter.get(0).timestampSeconds());
    Assertions.assertEquals(100L, ((Number) storageAfter.get(0).value().value()).longValue());

    List<MetricSample> s3After = sortedSamples(tableResult.afterMetrics().get("s3_cost"));
    Assertions.assertEquals(1, s3After.size());
    Assertions.assertEquals(10L, s3After.get(0).timestampSeconds());
    Assertions.assertEquals(1003L, ((Number) s3After.get(0).value().value()).longValue());

    EvaluationResult jobResult1 = findByScope(results, DataScope.Type.JOB, job1);
    checkJobMetrics(jobResult1.beforeMetrics(), jobResult1.afterMetrics());

    EvaluationResult jobResult2 = findByScope(results, DataScope.Type.JOB, job2);
    checkJobMetrics(jobResult2.beforeMetrics(), jobResult2.afterMetrics());
  }

  @Test
  void testPartitionMetrics() {
    long actionTime = 10;
    long rangeSeconds = 2;
    NameIdentifier tableIdentifier = NameIdentifier.of("db", "partitionTable");
    PartitionPath partitionPath =
        PartitionUtils.decodePartitionPath("[{\"country\":\"US\"},{\"region\":\"CA\"}]");

    updater.updateTableAndPartitionMetrics(
        Arrays.asList(
            MetricPoint.forPartition(
                tableIdentifier, partitionPath, "storage", StatisticValues.longValue(10), 8),
            MetricPoint.forPartition(
                tableIdentifier, partitionPath, "storage", StatisticValues.longValue(20), 11),
            MetricPoint.forPartition(
                tableIdentifier, partitionPath, "s3_cost", StatisticValues.longValue(5), 10)));

    List<EvaluationResult> results =
        monitor.evaluateMetrics(
            tableIdentifier, actionTime, rangeSeconds, Optional.of(partitionPath));

    EvaluationResult partitionResult =
        results.stream()
            .filter(result -> result.scope().type() == DataScope.Type.PARTITION)
            .findFirst()
            .orElseThrow(IllegalStateException::new);

    Assertions.assertEquals(partitionPath, partitionResult.scope().partition().orElseThrow());

    List<MetricSample> storageBefore =
        sortedSamples(partitionResult.beforeMetrics().get("storage"));
    Assertions.assertEquals(1, storageBefore.size());
    Assertions.assertEquals(8L, storageBefore.get(0).timestampSeconds());
    Assertions.assertEquals(10L, ((Number) storageBefore.get(0).value().value()).longValue());

    List<MetricSample> storageAfter = sortedSamples(partitionResult.afterMetrics().get("storage"));
    Assertions.assertEquals(1, storageAfter.size());
    Assertions.assertEquals(11L, storageAfter.get(0).timestampSeconds());
    Assertions.assertEquals(20L, ((Number) storageAfter.get(0).value().value()).longValue());

    List<MetricSample> s3After = sortedSamples(partitionResult.afterMetrics().get("s3_cost"));
    Assertions.assertEquals(1, s3After.size());
    Assertions.assertEquals(10L, s3After.get(0).timestampSeconds());
    Assertions.assertEquals(5L, ((Number) s3After.get(0).value().value()).longValue());
  }

  @Test
  void testMonitorCallbacks() {
    MonitorCallbackForTest.reset();
    NameIdentifier tableIdentifier = NameIdentifier.of("db", "table");

    monitor.evaluateMetrics(tableIdentifier, 10, 1, Optional.empty());

    Assertions.assertEquals(3, MonitorCallbackForTest.INVOCATIONS.get());
    Assertions.assertTrue(
        MonitorCallbackForTest.RESULTS.stream()
            .anyMatch(result -> result.scope().type() == DataScope.Type.TABLE));
  }

  private void checkJobMetrics(
      Map<String, List<MetricSample>> jobBeforeMetrics,
      Map<String, List<MetricSample>> jobAfterMetrics) {
    List<MetricSample> runtimeBefore = sortedSamples(jobBeforeMetrics.get("job_runtime"));
    Assertions.assertEquals(1, runtimeBefore.size());
    Assertions.assertEquals(8L, runtimeBefore.get(0).timestampSeconds());
    Assertions.assertEquals(8L, ((Number) runtimeBefore.get(0).value().value()).longValue());

    List<MetricSample> costBefore = sortedSamples(jobBeforeMetrics.get("job_cost"));
    Assertions.assertEquals(1, costBefore.size());
    Assertions.assertEquals(9L, costBefore.get(0).timestampSeconds());
    Assertions.assertEquals(9L, ((Number) costBefore.get(0).value().value()).longValue());

    List<MetricSample> runtimeAfter = sortedSamples(jobAfterMetrics.get("job_runtime"));
    Assertions.assertEquals(1, runtimeAfter.size());
    Assertions.assertEquals(11L, runtimeAfter.get(0).timestampSeconds());
    Assertions.assertEquals(12L, ((Number) runtimeAfter.get(0).value().value()).longValue());

    List<MetricSample> costAfter = sortedSamples(jobAfterMetrics.get("job_cost"));
    Assertions.assertEquals(1, costAfter.size());
    Assertions.assertEquals(10L, costAfter.get(0).timestampSeconds());
    Assertions.assertEquals(10L, ((Number) costAfter.get(0).value().value()).longValue());
  }

  private static EvaluationResult findByScope(
      List<EvaluationResult> results, DataScope.Type type, NameIdentifier identifier) {
    return results.stream()
        .filter(result -> result.scope().type() == type)
        .filter(result -> result.scope().identifier().equals(identifier))
        .findFirst()
        .orElseThrow(IllegalStateException::new);
  }

  private static List<MetricSample> sortedSamples(List<MetricSample> samples) {
    if (samples == null) {
      return List.of();
    }
    return samples.stream()
        .sorted(Comparator.comparingLong(MetricSample::timestampSeconds))
        .toList();
  }

  private OptimizerEnv createOptimizerEnv() {
    String jdbcUrl =
        String.format(
            "jdbc:h2:file:/tmp/gravitino-monitor-it-%d;DB_CLOSE_DELAY=-1;MODE=MYSQL;AUTO_SERVER=TRUE",
            System.nanoTime());

    Map<String, String> configs =
        ImmutableMap.<String, String>builder()
            .put(OptimizerConfig.METRICS_EVALUATOR_CONFIG.getKey(), MetricsEvaluatorForTest.NAME)
            .put(
                OptimizerConfig.TABLE_JOB_RELATION_PROVIDER_CONFIG.getKey(),
                TableJobRelationProviderForTest.NAME)
            .put(OptimizerConfig.MONITOR_CALLBACKS_CONFIG.getKey(), MonitorCallbackForTest.NAME)
            .put(
                OptimizerConfig.OPTIMIZER_PREFIX
                    + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX
                    + GenericJdbcMetricsRepository.JDBC_URL,
                jdbcUrl)
            .put(
                OptimizerConfig.OPTIMIZER_PREFIX
                    + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX
                    + GenericJdbcMetricsRepository.JDBC_DRIVER,
                "org.h2.Driver")
            .build();

    return new OptimizerEnv(new OptimizerConfig(configs));
  }
}
