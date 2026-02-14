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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.monitor.EvaluationResult;
import org.apache.gravitino.maintenance.optimizer.api.monitor.JobProvider;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricScope;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsEvaluator;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsProvider;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MonitorCallback;
import org.apache.gravitino.maintenance.optimizer.common.CloseableGroup;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.InstanceLoaderUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.ProviderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point that wires monitor providers, evaluator, and callbacks to assess optimization
 * outcomes from time-series metrics.
 *
 * <p>Purpose:
 *
 * <ul>
 *   <li>Fetch table (or partition) metrics in a window around an action timestamp.
 *   <li>Fetch related job metrics for the same time window.
 *   <li>Split metrics into before/after groups and evaluate them with {@link MetricsEvaluator}.
 *   <li>Publish each {@link EvaluationResult} to configured {@link MonitorCallback} instances.
 * </ul>
 *
 * <p>Configuration:
 *
 * <ul>
 *   <li>{@link OptimizerConfig#METRICS_PROVIDER_CONFIG} for {@link MetricsProvider}.
 *   <li>{@link OptimizerConfig#JOB_PROVIDER_CONFIG} for {@link JobProvider}.
 *   <li>{@link OptimizerConfig#METRICS_EVALUATOR_CONFIG} for {@link MetricsEvaluator}.
 *   <li>{@link OptimizerConfig#MONITOR_CALLBACKS_CONFIG} for callback list.
 * </ul>
 *
 * <p>Lifecycle:
 *
 * <ol>
 *   <li>Create a {@link Monitor} with an initialized {@link OptimizerEnv}.
 *   <li>The constructor resolves implementations via ServiceLoader and initializes them.
 *   <li>Call {@link #evaluateMetrics(NameIdentifier, long, long, Optional)} when needed.
 *   <li>Consume returned {@link EvaluationResult} list and callback side effects.
 *   <li>Call {@link #close()} to release provider/callback resources.
 * </ol>
 *
 * <p>Workflow:
 *
 * <ol>
 *   <li>Resolve a time range: {@code [actionTimeSeconds - rangeSeconds, actionTimeSeconds +
 *       rangeSeconds]}.
 *   <li>Read table/partition metrics and evaluate them.
 *   <li>Resolve related jobs from {@link JobProvider} and evaluate each job's metrics.
 *   <li>Return ordered results (table first, then jobs).
 * </ol>
 */
public class Monitor implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Monitor.class);
  private final MetricsProvider metricsProvider;
  private final JobProvider jobProvider;
  private final MetricsEvaluator metricsEvaluator;
  private final List<MonitorCallback> callbacks;
  private final CloseableGroup closeableGroup = new CloseableGroup();

  /**
   * Create a monitor by loading and initializing monitor providers, evaluator, and callbacks from
   * the supplied optimizer environment.
   *
   * @param optimizerEnv shared optimizer environment and configuration
   */
  public Monitor(OptimizerEnv optimizerEnv) {
    Preconditions.checkArgument(optimizerEnv != null, "optimizerEnv must not be null");
    this.metricsProvider = loadMetricsProvider(optimizerEnv.config());
    metricsProvider.initialize(optimizerEnv);
    closeableGroup.register(metricsProvider, MetricsProvider.class.getSimpleName());

    this.jobProvider = loadJobProvider(optimizerEnv.config());
    jobProvider.initialize(optimizerEnv);
    closeableGroup.register(jobProvider, JobProvider.class.getSimpleName());

    this.metricsEvaluator = loadMetricsEvaluator(optimizerEnv.config());
    metricsEvaluator.initialize(optimizerEnv);
    this.callbacks = loadCallbacks(optimizerEnv.config());
    for (MonitorCallback callback : callbacks) {
      callback.initialize(optimizerEnv);
      closeableGroup.register(callback, callback.name());
    }
  }

  /**
   * Evaluate table metrics and related job metrics around an action timestamp.
   *
   * @param tableIdentifier target table identifier
   * @param actionTimeSeconds action timestamp in epoch seconds
   * @param rangeSeconds half-window range in seconds, used to build [actionTimeSeconds-range,
   *     actionTimeSeconds+range]
   * @param partitionPath optional partition scope for table metrics
   * @return evaluation results, including one table/partition result and zero or more job results
   */
  public List<EvaluationResult> evaluateMetrics(
      NameIdentifier tableIdentifier,
      long actionTimeSeconds,
      long rangeSeconds,
      Optional<PartitionPath> partitionPath) {
    Preconditions.checkArgument(tableIdentifier != null, "tableIdentifier must not be null");
    Preconditions.checkArgument(partitionPath != null, "partitionPath must not be null");
    Preconditions.checkArgument(actionTimeSeconds >= 0, "actionTimeSeconds must be >= 0");
    Preconditions.checkArgument(rangeSeconds >= 0, "rangeSeconds must be >= 0");
    try {
      List<EvaluationResult> results = new ArrayList<>();
      results.add(
          evaluateTableMetrics(
              metricsEvaluator, tableIdentifier, actionTimeSeconds, rangeSeconds, partitionPath));
      List<NameIdentifier> jobs = jobProvider.jobIdentifiers(tableIdentifier);
      if (jobs == null) {
        jobs = List.of();
      }
      for (NameIdentifier jobIdentifier : jobs) {
        results.add(
            evaluateJobMetrics(metricsEvaluator, jobIdentifier, actionTimeSeconds, rangeSeconds));
      }
      return results;
    } catch (RuntimeException e) {
      if (e instanceof IllegalArgumentException) {
        throw e;
      }
      throw new IllegalStateException(
          String.format(
              "Failed to evaluate metrics for table=%s, actionTimeSeconds=%d, rangeSeconds=%d, partition=%s",
              tableIdentifier,
              actionTimeSeconds,
              rangeSeconds,
              partitionPath.map(PartitionPath::toString).orElse("<table-scope>")),
          e);
    }
  }

  private EvaluationResult evaluateTableMetrics(
      MetricsEvaluator evaluator,
      NameIdentifier tableIdentifier,
      long actionTimeSeconds,
      long rangeSeconds,
      Optional<PartitionPath> partitionPath) {
    Pair<Long, Long> timeRange = timeRange(actionTimeSeconds, rangeSeconds);
    Map<String, List<MetricSample>> metrics =
        partitionPath
            .map(
                path ->
                    metricsProvider.partitionMetrics(
                        tableIdentifier, path, timeRange.getLeft(), timeRange.getRight()))
            .orElseGet(
                () ->
                    metricsProvider.tableMetrics(
                        tableIdentifier, timeRange.getLeft(), timeRange.getRight()));

    Pair<Map<String, List<MetricSample>>, Map<String, List<MetricSample>>> splitMetrics =
        splitMetrics(metrics, actionTimeSeconds);

    MetricScope scope =
        partitionPath
            .map(path -> MetricScope.forPartition(tableIdentifier, path))
            .orElseGet(() -> MetricScope.forTable(tableIdentifier));
    boolean evaluation =
        evaluator.evaluateMetrics(scope, splitMetrics.getLeft(), splitMetrics.getRight());
    EvaluationResult result =
        new EvaluationResult(
            scope,
            evaluation,
            splitMetrics.getLeft(),
            splitMetrics.getRight(),
            actionTimeSeconds,
            rangeSeconds,
            evaluator.name());
    return notifyCallbacks(result);
  }

  private Pair<Map<String, List<MetricSample>>, Map<String, List<MetricSample>>> splitMetrics(
      Map<String, List<MetricSample>> metrics, long actionTimeInSeconds) {
    // split metrics into metrics before and after action time
    Map<String, List<MetricSample>> beforeMetrics = new HashMap<>();
    Map<String, List<MetricSample>> afterMetrics = new HashMap<>();
    Map<String, List<MetricSample>> source = metrics == null ? Collections.emptyMap() : metrics;
    for (Map.Entry<String, List<MetricSample>> entry : source.entrySet()) {
      String metricName = entry.getKey();
      List<MetricSample> metricList = entry.getValue() == null ? List.of() : entry.getValue();
      beforeMetrics.put(
          metricName,
          metricList.stream().filter(m -> m.timestamp() < actionTimeInSeconds).toList());
      afterMetrics.put(
          metricName,
          metricList.stream().filter(m -> m.timestamp() >= actionTimeInSeconds).toList());
    }
    return Pair.of(beforeMetrics, afterMetrics);
  }

  private EvaluationResult evaluateJobMetrics(
      MetricsEvaluator evaluator,
      NameIdentifier jobIdentifier,
      long actionTimeSeconds,
      long rangeSeconds) {
    Pair<Long, Long> timeRange = timeRange(actionTimeSeconds, rangeSeconds);
    Map<String, List<MetricSample>> metrics =
        metricsProvider.jobMetrics(jobIdentifier, timeRange.getLeft(), timeRange.getRight());
    Pair<Map<String, List<MetricSample>>, Map<String, List<MetricSample>>> splitMetrics =
        splitMetrics(metrics, actionTimeSeconds);
    MetricScope scope = MetricScope.forJob(jobIdentifier);
    boolean evaluation =
        evaluator.evaluateMetrics(scope, splitMetrics.getLeft(), splitMetrics.getRight());
    EvaluationResult result =
        new EvaluationResult(
            scope,
            evaluation,
            splitMetrics.getLeft(),
            splitMetrics.getRight(),
            actionTimeSeconds,
            rangeSeconds,
            evaluator.name());
    return notifyCallbacks(result);
  }

  private Pair<Long, Long> timeRange(long actionTimeSeconds, long rangeSeconds) {
    try {
      long startTime = Math.subtractExact(actionTimeSeconds, rangeSeconds);
      long endTime = Math.addExact(actionTimeSeconds, rangeSeconds);
      return Pair.of(startTime, endTime);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          String.format(
              "time range overflow: actionTimeSeconds=%d, rangeSeconds=%d",
              actionTimeSeconds, rangeSeconds),
          e);
    }
  }

  private MetricsProvider loadMetricsProvider(OptimizerConfig optimizerConfig) {
    return ProviderUtils.createMetricsProviderInstance(
        optimizerConfig.get(OptimizerConfig.METRICS_PROVIDER_CONFIG));
  }

  private JobProvider loadJobProvider(OptimizerConfig optimizerConfig) {
    return ProviderUtils.createJobProviderInstance(
        optimizerConfig.get(OptimizerConfig.JOB_PROVIDER_CONFIG));
  }

  private MetricsEvaluator loadMetricsEvaluator(OptimizerConfig optimizerConfig) {
    return InstanceLoaderUtils.createMetricsEvaluatorInstance(
        optimizerConfig.get(OptimizerConfig.METRICS_EVALUATOR_CONFIG));
  }

  private List<MonitorCallback> loadCallbacks(OptimizerConfig optimizerConfig) {
    List<String> callbackNames = optimizerConfig.get(OptimizerConfig.MONITOR_CALLBACKS_CONFIG);
    if (callbackNames == null || callbackNames.isEmpty()) {
      return Collections.emptyList();
    }
    List<MonitorCallback> result = new ArrayList<>();
    for (String callbackName : callbackNames) {
      result.add(ProviderUtils.createMonitorCallbackInstance(callbackName));
    }
    return List.copyOf(result);
  }

  private EvaluationResult notifyCallbacks(EvaluationResult result) {
    if (callbacks.isEmpty()) {
      return result;
    }
    for (MonitorCallback callback : callbacks) {
      try {
        callback.onEvaluation(result);
      } catch (Exception e) {
        LOG.warn(
            "Monitor callback {} failed for scope {}",
            callback.name(),
            result.scope().identifier(),
            e);
      }
    }
    return result;
  }

  /** Close all initialized monitor providers and callbacks. */
  @Override
  public void close() throws Exception {
    closeableGroup.close();
  }
}
