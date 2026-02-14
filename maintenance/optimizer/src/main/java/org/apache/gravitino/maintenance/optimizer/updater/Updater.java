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

package org.apache.gravitino.maintenance.optimizer.updater;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.DataScope;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.TableAndPartitionStatistics;
import org.apache.gravitino.maintenance.optimizer.api.updater.MetricsUpdater;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsCalculator;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsUpdater;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateBulkJobMetrics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateBulkTableMetrics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateBulkTableStatistics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateJobMetrics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateTableMetrics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateTableStatistics;
import org.apache.gravitino.maintenance.optimizer.common.CloseableGroup;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.InstanceLoaderUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.ProviderUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point that wires together the statistics calculator and updater providers to persist
 * optimizer statistics or metrics.
 *
 * <p>Usage:
 *
 * <ol>
 *   <li>Configure {@link OptimizerConfig#STATISTICS_UPDATER_CONFIG} and {@link
 *       OptimizerConfig#METRICS_UPDATER_CONFIG} with provider names.
 *   <li>Instantiate {@link Updater} with an {@link OptimizerEnv} to initialize the providers.
 *   <li>Call {@link #update(String, List, UpdateType)} for specific identifiers or {@link
 *       #updateAll(String, UpdateType)} for bulk refresh.
 *   <li>Call {@link #close()} to release provider resources.
 * </ol>
 */
public class Updater implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(Updater.class);

  private StatisticsUpdater statisticsUpdater;
  private MetricsUpdater metricsUpdater;
  private OptimizerEnv optimizerEnv;
  private final CloseableGroup closeableGroup = new CloseableGroup();

  public Updater(OptimizerEnv optimizerEnv) {
    this.optimizerEnv = optimizerEnv;
    this.statisticsUpdater = loadStatisticsUpdater(optimizerEnv.config());
    statisticsUpdater.initialize(optimizerEnv);
    closeableGroup.register(statisticsUpdater, StatisticsUpdater.class.getSimpleName());

    this.metricsUpdater = loadMetricsUpdater(optimizerEnv.config());
    metricsUpdater.initialize(optimizerEnv);
    closeableGroup.register(metricsUpdater, MetricsUpdater.class.getSimpleName());
  }

  /**
   * Updates statistics or metrics for the provided identifiers.
   *
   * @param statisticsCalculatorName The provider name of the statistics calculator.
   * @param nameIdentifiers The identifiers to update (table and/or job).
   * @param updateType The target update type: statistics or metrics.
   * @return summary of processed record counts by scope
   */
  public UpdateSummary update(
      String statisticsCalculatorName,
      List<NameIdentifier> nameIdentifiers,
      UpdateType updateType) {
    StatisticsCalculator calculator = getStatisticsCalculator(statisticsCalculatorName);

    if (UpdateType.STATISTICS.equals(updateType)) {
      return updateStatisticsForIdentifiers(statisticsCalculatorName, nameIdentifiers, calculator);
    }

    return updateMetricsForIdentifiers(statisticsCalculatorName, nameIdentifiers, calculator);
  }

  /**
   * Updates statistics or metrics for all identifiers returned by the calculator.
   *
   * @param statisticsCalculatorName The provider name of the statistics calculator.
   * @param updateType The target update type: statistics or metrics.
   * @return summary of processed record counts by scope
   */
  public UpdateSummary updateAll(String statisticsCalculatorName, UpdateType updateType) {
    StatisticsCalculator calculator = getStatisticsCalculator(statisticsCalculatorName);

    if (UpdateType.STATISTICS.equals(updateType)) {
      return updateAllStatistics(statisticsCalculatorName, calculator);
    }

    return updateAllMetrics(statisticsCalculatorName, calculator);
  }

  @VisibleForTesting
  public MetricsUpdater getMetricsUpdater() {
    return metricsUpdater;
  }

  @VisibleForTesting
  public StatisticsUpdater getStatisticsUpdater() {
    return statisticsUpdater;
  }

  public static final class UpdateSummary {
    private final UpdateType updateType;
    private final long totalRecords;
    private final long tableRecords;
    private final long partitionRecords;
    private final long jobRecords;

    private UpdateSummary(
        UpdateType updateType,
        long totalRecords,
        long tableRecords,
        long partitionRecords,
        long jobRecords) {
      this.updateType = updateType;
      this.totalRecords = totalRecords;
      this.tableRecords = tableRecords;
      this.partitionRecords = partitionRecords;
      this.jobRecords = jobRecords;
    }

    public UpdateType updateType() {
      return updateType;
    }

    public long totalRecords() {
      return totalRecords;
    }

    public long tableRecords() {
      return tableRecords;
    }

    public long partitionRecords() {
      return partitionRecords;
    }

    public long jobRecords() {
      return jobRecords;
    }
  }

  @Override
  public void close() throws Exception {
    closeableGroup.close();
  }

  private UpdateSummary updateStatisticsForIdentifiers(
      String statisticsCalculatorName,
      List<NameIdentifier> nameIdentifiers,
      StatisticsCalculator calculator) {
    long tableRecords = 0;
    long partitionRecords = 0;

    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      if (!(calculator instanceof SupportsCalculateTableStatistics)) {
        continue;
      }
      SupportsCalculateTableStatistics supportTableStatistics =
          (SupportsCalculateTableStatistics) calculator;
      TableAndPartitionStatistics bundle =
          supportTableStatistics.calculateTableStatistics(nameIdentifier);
      List<StatisticEntry<?>> statistics = bundle != null ? bundle.tableStatistics() : List.of();
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics =
          bundle != null ? bundle.partitionStatistics() : Map.of();

      tableRecords += countStatistics(statistics);
      partitionRecords += countPartitionStatistics(partitionStatistics);
      LOG.info(
          "Updating table statistics: calculator={}, identifier={}",
          statisticsCalculatorName,
          nameIdentifier);

      updateTableStatistics(statistics, nameIdentifier);
      updatePartitionStatistics(partitionStatistics, nameIdentifier);
    }
    return buildSummary(UpdateType.STATISTICS, tableRecords, partitionRecords, 0L);
  }

  private UpdateSummary updateMetricsForIdentifiers(
      String statisticsCalculatorName,
      List<NameIdentifier> nameIdentifiers,
      StatisticsCalculator calculator) {
    boolean hasTableMetricsCalculator = calculator instanceof SupportsCalculateTableMetrics;
    boolean hasJobMetricsCalculator = calculator instanceof SupportsCalculateJobMetrics;
    if (!hasTableMetricsCalculator && !hasJobMetricsCalculator) {
      throw new IllegalArgumentException(
          String.format(
              "Statistics calculator '%s' does not implement metric interfaces. "
                  + "Expected SupportsCalculateTableMetrics and/or SupportsCalculateJobMetrics.",
              statisticsCalculatorName));
    }

    long tableRecords = 0;
    long partitionRecords = 0;
    long jobRecords = 0;
    List<MetricPoint> pendingTableAndPartitionMetrics = new ArrayList<>();
    List<MetricPoint> pendingJobMetrics = new ArrayList<>();

    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      if (hasTableMetricsCalculator) {
        List<MetricPoint> metrics =
            ((SupportsCalculateTableMetrics) calculator).calculateTableMetrics(nameIdentifier);
        tableRecords += countMetricsByScope(metrics, DataScope.Type.TABLE);
        partitionRecords += countMetricsByScope(metrics, DataScope.Type.PARTITION);
        LOG.info(
            "Updating table/partition metrics: calculator={}, identifier={}, count={}",
            statisticsCalculatorName,
            nameIdentifier,
            metrics == null ? 0 : metrics.size());
        appendMetrics(pendingTableAndPartitionMetrics, metrics);
      }

      if (hasJobMetricsCalculator) {
        List<MetricPoint> metrics =
            ((SupportsCalculateJobMetrics) calculator).calculateJobMetrics(nameIdentifier);
        jobRecords += countMetricsByScope(metrics, DataScope.Type.JOB);
        LOG.info(
            "Updating job metrics: calculator={}, identifier={}, count={}",
            statisticsCalculatorName,
            nameIdentifier,
            metrics == null ? 0 : metrics.size());
        appendMetrics(pendingJobMetrics, metrics);
      }
    }

    updateTableAndPartitionMetrics(pendingTableAndPartitionMetrics);
    updateJobMetrics(pendingJobMetrics);
    return buildSummary(UpdateType.METRICS, tableRecords, partitionRecords, jobRecords);
  }

  private UpdateSummary updateAllStatistics(
      String statisticsCalculatorName, StatisticsCalculator calculator) {
    if (!(calculator instanceof SupportsCalculateBulkTableStatistics)) {
      throw new IllegalArgumentException(
          String.format(
              "Statistics calculator '%s' does not implement %s",
              statisticsCalculatorName,
              SupportsCalculateBulkTableStatistics.class.getSimpleName()));
    }

    Map<NameIdentifier, TableAndPartitionStatistics> allTableStatistics =
        ((SupportsCalculateBulkTableStatistics) calculator).calculateBulkTableStatistics();
    if (allTableStatistics == null) {
      allTableStatistics = Map.of();
    }

    long tableRecords = countAllTableStatistics(allTableStatistics);
    long partitionRecords = countAllPartitionStatistics(allTableStatistics);

    allTableStatistics.forEach(
        (identifier, bundle) -> {
          List<StatisticEntry<?>> statistics =
              bundle != null ? bundle.tableStatistics() : List.of();
          Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics =
              bundle != null ? bundle.partitionStatistics() : Map.of();
          updateTableStatistics(statistics, identifier);
          updatePartitionStatistics(partitionStatistics, identifier);
        });
    return buildSummary(UpdateType.STATISTICS, tableRecords, partitionRecords, 0L);
  }

  private UpdateSummary updateAllMetrics(
      String statisticsCalculatorName, StatisticsCalculator calculator) {
    boolean hasTableMetricsCalculator = calculator instanceof SupportsCalculateBulkTableMetrics;
    boolean hasJobMetricsCalculator = calculator instanceof SupportsCalculateBulkJobMetrics;
    if (!hasTableMetricsCalculator && !hasJobMetricsCalculator) {
      throw new IllegalArgumentException(
          String.format(
              "Statistics calculator '%s' does not implement bulk metric interfaces. "
                  + "Expected SupportsCalculateBulkTableMetrics and/or SupportsCalculateBulkJobMetrics.",
              statisticsCalculatorName));
    }

    long tableRecords = 0;
    long partitionRecords = 0;
    long jobRecords = 0;
    List<MetricPoint> pendingTableAndPartitionMetrics = new ArrayList<>();
    List<MetricPoint> pendingJobMetrics = new ArrayList<>();

    if (hasTableMetricsCalculator) {
      List<MetricPoint> metrics =
          ((SupportsCalculateBulkTableMetrics) calculator).calculateAllTableMetrics();
      tableRecords += countMetricsByScope(metrics, DataScope.Type.TABLE);
      partitionRecords += countMetricsByScope(metrics, DataScope.Type.PARTITION);
      appendMetrics(pendingTableAndPartitionMetrics, metrics);
    }

    if (hasJobMetricsCalculator) {
      List<MetricPoint> metrics =
          ((SupportsCalculateBulkJobMetrics) calculator).calculateAllJobMetrics();
      jobRecords += countMetricsByScope(metrics, DataScope.Type.JOB);
      appendMetrics(pendingJobMetrics, metrics);
    }

    updateTableAndPartitionMetrics(pendingTableAndPartitionMetrics);
    updateJobMetrics(pendingJobMetrics);
    return buildSummary(UpdateType.METRICS, tableRecords, partitionRecords, jobRecords);
  }

  private void updateTableStatistics(
      List<StatisticEntry<?>> statistics, NameIdentifier tableIdentifier) {
    LOG.info(
        "Persisting table statistics: identifier={}, count={}, details={}",
        tableIdentifier,
        statistics != null ? statistics.size() : 0,
        summarize(statistics));
    statisticsUpdater.updateTableStatistics(tableIdentifier, statistics);
  }

  private void updatePartitionStatistics(
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics,
      NameIdentifier tableIdentifier) {
    if (partitionStatistics == null || partitionStatistics.isEmpty()) {
      LOG.info(
          "Persist partition statistics skipped: identifier={}, reason=empty partitions",
          tableIdentifier);
      return;
    }
    LOG.info(
        "Persisting partition statistics: identifier={}, partitions={}, names={}, sample={}",
        tableIdentifier,
        partitionStatistics.size(),
        partitionNames(partitionStatistics),
        summarize(
            partitionStatistics.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList())));
    statisticsUpdater.updatePartitionStatistics(tableIdentifier, partitionStatistics);
  }

  private void updateTableAndPartitionMetrics(List<MetricPoint> metrics) {
    if (metrics == null || metrics.isEmpty()) {
      return;
    }
    metricsUpdater.updateTableAndPartitionMetrics(metrics);
  }

  private void updateJobMetrics(List<MetricPoint> metrics) {
    if (metrics == null || metrics.isEmpty()) {
      return;
    }
    metricsUpdater.updateJobMetrics(metrics);
  }

  private void appendMetrics(List<MetricPoint> pendingMetrics, List<MetricPoint> metricsToAppend) {
    if (metricsToAppend == null || metricsToAppend.isEmpty()) {
      return;
    }
    pendingMetrics.addAll(metricsToAppend);
  }

  private String summarize(List<StatisticEntry<?>> statistics) {
    if (statistics == null || statistics.isEmpty()) {
      return "[]";
    }
    int limit = Math.min(statistics.size(), 20);
    String summary =
        statistics.stream()
            .limit(limit)
            .map(stat -> stat.name() + "=" + stat.value().value())
            .collect(Collectors.joining(", ", "[", "]"));
    if (statistics.size() > limit) {
      summary = summary + " ... (" + statistics.size() + " total)";
    }
    return summary;
  }

  private StatisticsCalculator getStatisticsCalculator(String statisticsCalculatorName) {
    StatisticsCalculator calculator =
        InstanceLoaderUtils.createStatisticsCalculatorInstance(statisticsCalculatorName);
    calculator.initialize(optimizerEnv);
    return calculator;
  }

  private StatisticsUpdater loadStatisticsUpdater(OptimizerConfig config) {
    String statisticsUpdaterName = config.get(OptimizerConfig.STATISTICS_UPDATER_CONFIG);
    if (statisticsUpdaterName == null || statisticsUpdaterName.isBlank()) {
      throw new IllegalArgumentException(
          "Statistics updater provider name is required. Set "
              + OptimizerConfig.STATISTICS_UPDATER_CONFIG.getKey()
              + " to a valid provider name.");
    }
    return ProviderUtils.createStatisticsUpdaterInstance(statisticsUpdaterName);
  }

  private String partitionNames(Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics) {
    return partitionStatistics.keySet().stream()
        .map(PartitionUtils::encodePartitionPath)
        .collect(Collectors.joining(", ", "[", "]"));
  }

  private MetricsUpdater loadMetricsUpdater(OptimizerConfig config) {
    String metricsUpdaterName = config.get(OptimizerConfig.METRICS_UPDATER_CONFIG);
    if (metricsUpdaterName == null || metricsUpdaterName.isBlank()) {
      throw new IllegalArgumentException(
          "Metrics updater provider name is required. Set "
              + OptimizerConfig.METRICS_UPDATER_CONFIG.getKey()
              + " to a valid provider name.");
    }
    return ProviderUtils.createMetricsUpdaterInstance(metricsUpdaterName);
  }

  private long countStatistics(List<StatisticEntry<?>> statistics) {
    return statistics == null ? 0 : statistics.size();
  }

  private long countPartitionStatistics(
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics) {
    if (partitionStatistics == null) {
      return 0;
    }
    return partitionStatistics.values().stream().mapToLong(this::countStatistics).sum();
  }

  private long countAllTableStatistics(
      Map<NameIdentifier, TableAndPartitionStatistics> statisticsByTable) {
    if (statisticsByTable == null) {
      return 0;
    }
    return statisticsByTable.values().stream()
        .mapToLong(bundle -> countStatistics(bundle.tableStatistics()))
        .sum();
  }

  private long countAllPartitionStatistics(
      Map<NameIdentifier, TableAndPartitionStatistics> partitionStatisticsByTable) {
    if (partitionStatisticsByTable == null) {
      return 0;
    }
    return partitionStatisticsByTable.values().stream()
        .mapToLong(bundle -> countPartitionStatistics(bundle.partitionStatistics()))
        .sum();
  }

  private long countMetricsByScope(List<MetricPoint> metrics, DataScope.Type scope) {
    if (metrics == null || metrics.isEmpty()) {
      return 0;
    }
    return metrics.stream().filter(metric -> metric.scope() == scope).count();
  }

  private UpdateSummary buildSummary(
      UpdateType updateType, long tableRecords, long partitionRecords, long jobRecords) {
    return new UpdateSummary(
        updateType,
        tableRecords + partitionRecords + jobRecords,
        tableRecords,
        partitionRecords,
        jobRecords);
  }
}
