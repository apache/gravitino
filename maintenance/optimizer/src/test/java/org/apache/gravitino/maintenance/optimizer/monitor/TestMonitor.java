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

package org.apache.gravitino.maintenance.optimizer.monitor;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSeries;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricValueSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.monitor.EvaluationResult;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricScope;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.monitor.callback.MonitorCallbackForTest;
import org.apache.gravitino.maintenance.optimizer.monitor.evaluator.GravitinoMetricsEvaluator;
import org.apache.gravitino.maintenance.optimizer.monitor.evaluator.MetricsEvaluatorForTest;
import org.apache.gravitino.maintenance.optimizer.monitor.job.TableJobRelationProviderForTest;
import org.apache.gravitino.maintenance.optimizer.monitor.metrics.MetricsProviderForTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMonitor {

  @Test
  public void testEvaluateMetrics() throws Exception {
    OptimizerConfig config =
        new OptimizerConfig(
            ImmutableMap.<String, String>builder()
                .put(OptimizerConfig.METRICS_PROVIDER_CONFIG.getKey(), MetricsProviderForTest.NAME)
                .put(
                    OptimizerConfig.TABLE_JOB_RELATION_PROVIDER_CONFIG.getKey(),
                    TableJobRelationProviderForTest.NAME)
                .put(
                    OptimizerConfig.METRICS_EVALUATOR_CONFIG.getKey(), MetricsEvaluatorForTest.NAME)
                .put(OptimizerConfig.MONITOR_CALLBACKS_CONFIG.getKey(), MonitorCallbackForTest.NAME)
                .build());

    OptimizerEnv env = new OptimizerEnv(config);

    MonitorCallbackForTest.reset();
    MetricsEvaluatorForTest.reset();
    MetricsEvaluatorForTest.failJob2(true);
    MetricsProviderForTest.reset();

    NameIdentifier tableIdentifier = NameIdentifier.parse("test.db.table");
    List<EvaluationResult> results;
    try (Monitor monitor = new Monitor(env)) {
      results = monitor.evaluateMetrics(tableIdentifier, 100L, 10L, Optional.empty());
    }

    Assertions.assertEquals(3, results.size());
    Assertions.assertEquals(3, MetricsEvaluatorForTest.INVOCATIONS.get());
    Assertions.assertEquals(3, MonitorCallbackForTest.INVOCATIONS.get());
    Assertions.assertEquals(3, MonitorCallbackForTest.RESULTS.size());

    EvaluationResult tableResult = results.get(0);
    EvaluationResult jobResult1 = results.get(1);
    EvaluationResult jobResult2 = results.get(2);

    Assertions.assertEquals(MetricScope.Type.TABLE, tableResult.scope().type());
    Assertions.assertEquals(tableIdentifier, tableResult.scope().identifier());
    Assertions.assertTrue(tableResult.evaluation());
    Assertions.assertEquals(100L, tableResult.actionTimeSeconds());
    Assertions.assertEquals(10L, tableResult.rangeSeconds());
    Assertions.assertEquals(MetricsEvaluatorForTest.NAME, tableResult.evaluatorName());
    MetricValueSample tableBeforeRowCount = onlyMetric(tableResult.beforeSeries(), "row_count");
    MetricValueSample tableAfterRowCount = onlyMetric(tableResult.afterSeries(), "row_count");
    Assertions.assertEquals(95L, tableBeforeRowCount.timestampSeconds());
    Assertions.assertEquals(100L, tableAfterRowCount.timestampSeconds());
    Assertions.assertEquals(100L, metricLongValue(tableBeforeRowCount));
    Assertions.assertEquals(200L, metricLongValue(tableAfterRowCount));

    Assertions.assertEquals(MetricScope.Type.JOB, jobResult1.scope().type());
    Assertions.assertEquals(TableJobRelationProviderForTest.JOB1, jobResult1.scope().identifier());
    Assertions.assertTrue(jobResult1.evaluation());
    MetricValueSample job1BeforeDuration = onlyMetric(jobResult1.beforeSeries(), "duration");
    MetricValueSample job1AfterDuration = onlyMetric(jobResult1.afterSeries(), "duration");
    Assertions.assertEquals(99L, job1BeforeDuration.timestampSeconds());
    Assertions.assertEquals(102L, job1AfterDuration.timestampSeconds());
    Assertions.assertEquals(10L, metricLongValue(job1BeforeDuration));
    Assertions.assertEquals(20L, metricLongValue(job1AfterDuration));

    Assertions.assertEquals(MetricScope.Type.JOB, jobResult2.scope().type());
    Assertions.assertEquals(TableJobRelationProviderForTest.JOB2, jobResult2.scope().identifier());
    Assertions.assertFalse(jobResult2.evaluation());
    MetricValueSample job2BeforeDuration = onlyMetric(jobResult2.beforeSeries(), "duration");
    MetricValueSample job2AfterDuration = onlyMetric(jobResult2.afterSeries(), "duration");
    Assertions.assertEquals(98L, job2BeforeDuration.timestampSeconds());
    Assertions.assertEquals(104L, job2AfterDuration.timestampSeconds());
    Assertions.assertEquals(30L, metricLongValue(job2BeforeDuration));
    Assertions.assertEquals(40L, metricLongValue(job2AfterDuration));
  }

  @Test
  public void testEvaluateMetricsForPartition() throws Exception {
    OptimizerConfig config =
        new OptimizerConfig(
            ImmutableMap.<String, String>builder()
                .put(OptimizerConfig.METRICS_PROVIDER_CONFIG.getKey(), MetricsProviderForTest.NAME)
                .put(
                    OptimizerConfig.TABLE_JOB_RELATION_PROVIDER_CONFIG.getKey(),
                    TableJobRelationProviderForTest.NAME)
                .put(
                    OptimizerConfig.METRICS_EVALUATOR_CONFIG.getKey(), MetricsEvaluatorForTest.NAME)
                .put(OptimizerConfig.MONITOR_CALLBACKS_CONFIG.getKey(), MonitorCallbackForTest.NAME)
                .build());

    OptimizerEnv env = new OptimizerEnv(config);

    MonitorCallbackForTest.reset();
    MetricsEvaluatorForTest.reset();
    MetricsEvaluatorForTest.failJob2(false);
    MetricsProviderForTest.reset();

    NameIdentifier tableIdentifier = NameIdentifier.parse("test.db.table");
    PartitionPath partitionPath =
        PartitionPath.of(List.of(new PartitionEntryImpl("dt", "2026-02-12")));
    List<EvaluationResult> results;
    try (Monitor monitor = new Monitor(env)) {
      results = monitor.evaluateMetrics(tableIdentifier, 100L, 10L, Optional.of(partitionPath));
    }

    Assertions.assertEquals(1, MetricsProviderForTest.PARTITION_METRICS_CALLS.get());
    Assertions.assertEquals(partitionPath, MetricsProviderForTest.LAST_PARTITION_PATH);
    Assertions.assertEquals(3, results.size());

    EvaluationResult tableResult = results.get(0);
    Assertions.assertEquals(MetricScope.Type.PARTITION, tableResult.scope().type());
    Assertions.assertEquals(partitionPath, tableResult.scope().partition().orElseThrow());
    MetricValueSample beforeRowCount = onlyMetric(tableResult.beforeSeries(), "row_count");
    MetricValueSample afterRowCount = onlyMetric(tableResult.afterSeries(), "row_count");
    Assertions.assertEquals(97L, beforeRowCount.timestampSeconds());
    Assertions.assertEquals(101L, afterRowCount.timestampSeconds());
    Assertions.assertEquals(110L, metricLongValue(beforeRowCount));
    Assertions.assertEquals(210L, metricLongValue(afterRowCount));
  }

  @Test
  public void testEvaluateMetricsOverflowRange() throws Exception {
    OptimizerConfig config =
        new OptimizerConfig(
            ImmutableMap.<String, String>builder()
                .put(OptimizerConfig.METRICS_PROVIDER_CONFIG.getKey(), MetricsProviderForTest.NAME)
                .put(
                    OptimizerConfig.TABLE_JOB_RELATION_PROVIDER_CONFIG.getKey(),
                    TableJobRelationProviderForTest.NAME)
                .put(
                    OptimizerConfig.METRICS_EVALUATOR_CONFIG.getKey(), MetricsEvaluatorForTest.NAME)
                .put(OptimizerConfig.MONITOR_CALLBACKS_CONFIG.getKey(), MonitorCallbackForTest.NAME)
                .build());
    OptimizerEnv env = new OptimizerEnv(config);

    try (Monitor monitor = new Monitor(env)) {
      IllegalArgumentException exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  monitor.evaluateMetrics(
                      NameIdentifier.parse("test.db.table"), Long.MAX_VALUE, 1L, Optional.empty()));
      Assertions.assertTrue(exception.getMessage().contains("time range overflow"));
    }
  }

  @Test
  public void testEvaluateMetricsWithScopedGravitinoEvaluatorRules() throws Exception {
    OptimizerConfig config =
        new OptimizerConfig(
            ImmutableMap.<String, String>builder()
                .put(OptimizerConfig.METRICS_PROVIDER_CONFIG.getKey(), MetricsProviderForTest.NAME)
                .put(
                    OptimizerConfig.TABLE_JOB_RELATION_PROVIDER_CONFIG.getKey(),
                    TableJobRelationProviderForTest.NAME)
                .put(
                    OptimizerConfig.METRICS_EVALUATOR_CONFIG.getKey(),
                    GravitinoMetricsEvaluator.NAME)
                .put(
                    GravitinoMetricsEvaluator.EVALUATION_RULES_CONFIG,
                    "table:row_count:avg:le,job:duration:latest:le")
                .build());

    OptimizerEnv env = new OptimizerEnv(config);
    NameIdentifier tableIdentifier = NameIdentifier.parse("test.db.table");

    List<EvaluationResult> results;
    try (Monitor monitor = new Monitor(env)) {
      results = monitor.evaluateMetrics(tableIdentifier, 100L, 10L, Optional.empty());
    }

    Assertions.assertEquals(3, results.size(), "Expected one table result and two job results");
    Assertions.assertFalse(results.get(0).evaluation(), "Table rule should fail for test metrics");
    Assertions.assertFalse(results.get(1).evaluation(), "Job1 latest duration rule should fail");
    Assertions.assertFalse(results.get(2).evaluation(), "Job2 latest duration rule should fail");
  }

  @Test
  public void testEvaluateMetricsSkipsInvalidScopeMetricPoints() throws Exception {
    OptimizerConfig config =
        new OptimizerConfig(
            ImmutableMap.<String, String>builder()
                .put(OptimizerConfig.METRICS_PROVIDER_CONFIG.getKey(), MetricsProviderForTest.NAME)
                .put(
                    OptimizerConfig.TABLE_JOB_RELATION_PROVIDER_CONFIG.getKey(),
                    TableJobRelationProviderForTest.NAME)
                .put(
                    OptimizerConfig.METRICS_EVALUATOR_CONFIG.getKey(), MetricsEvaluatorForTest.NAME)
                .build());
    OptimizerEnv env = new OptimizerEnv(config);

    MetricsEvaluatorForTest.reset();
    MetricsProviderForTest.reset();
    MetricsProviderForTest.includeInvalidScopeMetric(true);

    List<EvaluationResult> results;
    try (Monitor monitor = new Monitor(env)) {
      results =
          monitor.evaluateMetrics(
              NameIdentifier.parse("test.db.table"), 100L, 10L, Optional.empty());
    }

    EvaluationResult tableResult = results.get(0);
    MetricValueSample beforeRowCount = onlyMetric(tableResult.beforeSeries(), "row_count");
    Assertions.assertEquals(95L, beforeRowCount.timestampSeconds());
  }

  private static MetricValueSample onlyMetric(MetricSeries series, String metricName) {
    List<MetricValueSample> matched = series.samples(metricName);
    Assertions.assertEquals(1, matched.size());
    return matched.get(0);
  }

  private static long metricLongValue(MetricValueSample metricValueSample) {
    return ((Number) metricValueSample.value().value()).longValue();
  }
}
