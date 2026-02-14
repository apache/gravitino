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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsProvider;
import org.apache.gravitino.maintenance.optimizer.common.MetricSampleImpl;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.PartitionMetricSampleImpl;
import org.apache.gravitino.maintenance.optimizer.common.util.StatisticValueUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.maintenance.optimizer.updater.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.H2MetricsStorage;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricRecord;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricsRepository;

/**
 * {@link MetricsProvider} implementation backed by Gravitino metric storage.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li>Set {@link org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig
 *       #METRICS_PROVIDER_CONFIG} to {@value #NAME}.
 *   <li>This provider initializes an internal {@link H2MetricsStorage} and reads metrics through
 *       {@link MetricsRepository} APIs.
 * </ul>
 *
 * <p>Behavior:
 *
 * <ul>
 *   <li>{@link #jobMetrics(NameIdentifier, long, long)} returns job metrics in the requested time
 *       range.
 *   <li>{@link #tableMetrics(NameIdentifier, long, long)} returns table metrics in the requested
 *       time range.
 *   <li>{@link #partitionMetrics(NameIdentifier, PartitionPath, long, long)} returns partition
 *       metrics using encoded partition path.
 *   <li>Storage records are converted to monitor-domain {@link MetricSample} values.
 * </ul>
 */
public class GravitinoMetricsProvider implements MetricsProvider {

  public static final String NAME = "gravitino-metrics-provider";
  private MetricsRepository metricsStorage;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    this.metricsStorage = new H2MetricsStorage();
    metricsStorage.initialize(optimizerEnv.config().getAllConfig());
  }

  @Override
  public Map<String, List<MetricSample>> jobMetrics(
      NameIdentifier jobIdentifier, long startTime, long endTime) {
    ensureInitialized();
    Map<String, List<MetricRecord>> metrics =
        metricsStorage.getJobMetrics(jobIdentifier, startTime, endTime);

    return toSingleMetrics(metrics, Optional.empty());
  }

  @Override
  public Map<String, List<MetricSample>> tableMetrics(
      NameIdentifier tableIdentifier, long startTime, long endTime) {
    ensureInitialized();
    Map<String, List<MetricRecord>> metrics =
        metricsStorage.getTableMetrics(tableIdentifier, startTime, endTime);

    return toSingleMetrics(metrics, Optional.empty());
  }

  @Override
  public Map<String, List<MetricSample>> partitionMetrics(
      NameIdentifier tableIdentifier, PartitionPath partitionPath, long startTime, long endTime) {
    ensureInitialized();
    Map<String, List<MetricRecord>> metrics =
        metricsStorage.getPartitionMetrics(
            tableIdentifier, PartitionUtils.encodePartitionPath(partitionPath), startTime, endTime);

    return toSingleMetrics(metrics, Optional.of(partitionPath));
  }

  private Map<String, List<MetricSample>> toSingleMetrics(
      Map<String, List<MetricRecord>> metrics, Optional<PartitionPath> partitionPath) {
    return metrics.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> entry.getKey(),
                entry ->
                    entry.getValue().stream()
                        .map(
                            storageMetric ->
                                toSingleMetric(storageMetric, partitionPath, entry.getKey()))
                        .collect(Collectors.toList())));
  }

  private MetricSample toSingleMetric(
      MetricRecord metric, Optional<PartitionPath> partitionPath, String metricName) {
    StatisticEntry<?> statistic =
        new StatisticEntryImpl<>(metricName, StatisticValueUtils.fromString(metric.getValue()));
    return partitionPath
        .<MetricSample>map(
            partition -> new PartitionMetricSampleImpl(metric.getTimestamp(), statistic, partition))
        .orElseGet(() -> new MetricSampleImpl(metric.getTimestamp(), statistic));
  }

  private void ensureInitialized() {
    Preconditions.checkState(metricsStorage != null, "GravitinoMetricsProvider is not initialized");
  }

  @Override
  public void close() throws Exception {
    if (metricsStorage != null) {
      metricsStorage.close();
    }
  }
}
