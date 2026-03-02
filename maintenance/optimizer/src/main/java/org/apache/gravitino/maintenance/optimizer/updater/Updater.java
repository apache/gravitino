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
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.TableAndPartitionStatistics;
import org.apache.gravitino.maintenance.optimizer.api.updater.MetricsUpdater;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsCalculator;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsUpdater;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateBulkJobStatistics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateBulkTableStatistics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateJobStatistics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateTableStatistics;
import org.apache.gravitino.maintenance.optimizer.common.CloseableGroup;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.InstanceLoaderUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.ProviderUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.StatisticValueUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.JobMetricWriteRequest;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricRecordImpl;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.TableMetricWriteRequest;
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
   * <p>This is the main entry point for updating a bounded set of targets. The updater resolves the
   * {@link StatisticsCalculator} by name, calculates table and partition statistics, and persists
   * either raw statistics or derived metrics based on {@code updateType}. If the calculator
   * implements {@link SupportsCalculateJobStatistics} and {@code updateType} is {@link
   * UpdateType#METRICS}, job metrics are also emitted.
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
    List<TableMetricWriteRequest> tableMetricWriteRequests = new ArrayList<>();
    List<JobMetricWriteRequest> jobMetricWriteRequests = new ArrayList<>();
    long tableRecords = 0;
    long partitionRecords = 0;
    long jobRecords = 0;
    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      if (calculator instanceof SupportsCalculateTableStatistics) {
        SupportsCalculateTableStatistics supportTableStatistics =
            ((SupportsCalculateTableStatistics) calculator);
        TableAndPartitionStatistics bundle =
            supportTableStatistics.calculateTableStatistics(nameIdentifier);
        List<StatisticEntry<?>> statistics = bundle != null ? bundle.tableStatistics() : List.of();
        Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics =
            bundle != null ? bundle.partitionStatistics() : Map.of();
        tableRecords += countStatistics(statistics);
        partitionRecords += countPartitionStatistics(partitionStatistics);
        LOG.info(
            "Updating table statistics/metrics: calculator={}, updateType={}, identifier={}",
            statisticsCalculatorName,
            updateType,
            nameIdentifier);
        if (UpdateType.STATISTICS.equals(updateType)) {
          updateTableStatistics(statistics, nameIdentifier);
          updatePartitionStatistics(partitionStatistics, nameIdentifier);
        } else {
          tableMetricWriteRequests.addAll(collectTableMetrics(statistics, nameIdentifier));
          tableMetricWriteRequests.addAll(
              collectPartitionMetrics(partitionStatistics, nameIdentifier));
        }
      }
      if (calculator instanceof SupportsCalculateJobStatistics
          && UpdateType.METRICS.equals(updateType)) {
        SupportsCalculateJobStatistics supportJobStatistics =
            ((SupportsCalculateJobStatistics) calculator);
        List<StatisticEntry<?>> statistics =
            supportJobStatistics.calculateJobStatistics(nameIdentifier);
        jobRecords += countStatistics(statistics);
        LOG.info(
            "Updating job metrics: calculator={}, identifier={}",
            statisticsCalculatorName,
            nameIdentifier);
        jobMetricWriteRequests.addAll(collectJobMetrics(statistics, nameIdentifier));
      }
    }
    updateTableAndJobMetrics(tableMetricWriteRequests, jobMetricWriteRequests);
    return buildSummary(updateType, tableRecords, partitionRecords, jobRecords);
  }

  /**
   * Updates statistics or metrics for all identifiers returned by the calculator.
   *
   * <p>This is the main entry point for batch refreshes. The updater asks the {@link
   * StatisticsCalculator} for all table statistics (and optionally job statistics) and persists
   * them according to {@code updateType}. If the calculator implements {@link
   * SupportsCalculateBulkJobStatistics} and {@code updateType} is {@link UpdateType#METRICS}, job
   * metrics are also emitted.
   *
   * @param statisticsCalculatorName The provider name of the statistics calculator.
   * @param updateType The target update type: statistics or metrics.
   * @return summary of processed record counts by scope
   */
  public UpdateSummary updateAll(String statisticsCalculatorName, UpdateType updateType) {
    StatisticsCalculator calculator = getStatisticsCalculator(statisticsCalculatorName);
    List<TableMetricWriteRequest> tableMetricWriteRequests = new ArrayList<>();
    List<JobMetricWriteRequest> jobMetricWriteRequests = new ArrayList<>();
    long tableRecords = 0;
    long partitionRecords = 0;
    long jobRecords = 0;

    if (calculator instanceof SupportsCalculateBulkTableStatistics supportBulkTableStatistics) {
      Map<NameIdentifier, TableAndPartitionStatistics> allTableStatistics =
          supportBulkTableStatistics.calculateBulkTableStatistics();
      if (allTableStatistics == null) {
        allTableStatistics = Map.of();
      }

      tableRecords += countAllTableStatistics(allTableStatistics);
      partitionRecords += countAllPartitionStatistics(allTableStatistics);
      allTableStatistics.forEach(
          (identifier, bundle) -> {
            List<StatisticEntry<?>> statistics =
                bundle != null ? bundle.tableStatistics() : List.of();
            Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics =
                bundle != null ? bundle.partitionStatistics() : Map.of();
            if (UpdateType.STATISTICS.equals(updateType)) {
              updateTableStatistics(statistics, identifier);
              updatePartitionStatistics(partitionStatistics, identifier);
            } else {
              tableMetricWriteRequests.addAll(collectTableMetrics(statistics, identifier));
              tableMetricWriteRequests.addAll(
                  collectPartitionMetrics(partitionStatistics, identifier));
            }
          });
    }

    if (calculator instanceof SupportsCalculateBulkJobStatistics supportJobStatistics
        && UpdateType.METRICS.equals(updateType)) {
      Map<NameIdentifier, List<StatisticEntry<?>>> allJobStatistics =
          supportJobStatistics.calculateAllJobStatistics();
      if (allJobStatistics == null) {
        allJobStatistics = Map.of();
      }
      jobRecords += countAllStatistics(allJobStatistics);
      allJobStatistics.forEach(
          (identifier, statistics) ->
              jobMetricWriteRequests.addAll(collectJobMetrics(statistics, identifier)));
    }
    updateTableAndJobMetrics(tableMetricWriteRequests, jobMetricWriteRequests);
    return buildSummary(updateType, tableRecords, partitionRecords, jobRecords);
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
        summarize(partitionStatistics.values().stream().flatMap(Collection::stream).toList()));
    statisticsUpdater.updatePartitionStatistics(tableIdentifier, partitionStatistics);
  }

  private List<TableMetricWriteRequest> collectTableMetrics(
      List<StatisticEntry<?>> statistics, NameIdentifier tableIdentifier) {
    long timestampSeconds = System.currentTimeMillis() / 1000;
    LOG.info(
        "Persisting table metrics: identifier={}, count={}, details={}",
        tableIdentifier,
        statistics != null ? statistics.size() : 0,
        summarize(statistics));
    return toTableMetricWriteRequests(tableIdentifier, statistics, timestampSeconds);
  }

  private List<TableMetricWriteRequest> collectPartitionMetrics(
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics,
      NameIdentifier tableIdentifier) {
    if (partitionStatistics == null || partitionStatistics.isEmpty()) {
      LOG.info(
          "Persist partition metrics skipped: identifier={}, reason=empty partitions",
          tableIdentifier);
      return List.of();
    }
    long timestampSeconds = System.currentTimeMillis() / 1000;
    LOG.info(
        "Persisting partition metrics: identifier={}, partitions={}, names={}, details={}",
        tableIdentifier,
        partitionStatistics.size(),
        partitionNames(partitionStatistics),
        summarize(partitionStatistics.values().stream().flatMap(Collection::stream).toList()));
    return toPartitionMetricWriteRequests(tableIdentifier, partitionStatistics, timestampSeconds);
  }

  private List<JobMetricWriteRequest> collectJobMetrics(
      List<StatisticEntry<?>> statistics, NameIdentifier jobIdentifier) {
    long timestampSeconds = System.currentTimeMillis() / 1000;

    LOG.info(
        "Persisting job metrics: identifier={}, count={}, details={}",
        jobIdentifier,
        statistics != null ? statistics.size() : 0,
        summarize(statistics));
    return toJobMetricWriteRequests(jobIdentifier, statistics, timestampSeconds);
  }

  private void updateTableAndJobMetrics(
      List<TableMetricWriteRequest> tableMetricWriteRequests,
      List<JobMetricWriteRequest> jobMetricWriteRequests) {
    if (!tableMetricWriteRequests.isEmpty()) {
      metricsUpdater.updateTableMetrics(tableMetricWriteRequests);
    }
    if (!jobMetricWriteRequests.isEmpty()) {
      metricsUpdater.updateJobMetrics(jobMetricWriteRequests);
    }
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

  private List<TableMetricWriteRequest> toTableMetricWriteRequests(
      NameIdentifier tableIdentifier, List<StatisticEntry<?>> statistics, long timestamp) {
    List<TableMetricWriteRequest> metrics = new ArrayList<>();
    if (statistics != null) {
      statistics.forEach(
          stat ->
              metrics.add(
                  new TableMetricWriteRequest(
                      tableIdentifier,
                      stat.name(),
                      Optional.empty(),
                      new MetricRecordImpl(
                          timestamp, StatisticValueUtils.toString(stat.value())))));
    }
    return metrics;
  }

  private List<TableMetricWriteRequest> toPartitionMetricWriteRequests(
      NameIdentifier tableIdentifier,
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics,
      long timestamp) {
    List<TableMetricWriteRequest> metrics = new ArrayList<>();
    if (partitionStatistics != null) {
      partitionStatistics.forEach(
          (partitionPath, statisticEntries) ->
              statisticEntries.forEach(
                  stat ->
                      metrics.add(
                          new TableMetricWriteRequest(
                              tableIdentifier,
                              stat.name(),
                              Optional.of(PartitionUtils.encodePartitionPath(partitionPath)),
                              new MetricRecordImpl(
                                  timestamp, StatisticValueUtils.toString(stat.value()))))));
    }
    return metrics;
  }

  private List<JobMetricWriteRequest> toJobMetricWriteRequests(
      NameIdentifier jobIdentifier, List<StatisticEntry<?>> statistics, long timestamp) {
    List<JobMetricWriteRequest> metrics = new ArrayList<>();
    if (statistics != null) {
      statistics.forEach(
          stat ->
              metrics.add(
                  new JobMetricWriteRequest(
                      jobIdentifier,
                      stat.name(),
                      new MetricRecordImpl(
                          timestamp, StatisticValueUtils.toString(stat.value())))));
    }
    return metrics;
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

  private long countAllStatistics(Map<NameIdentifier, List<StatisticEntry<?>>> statisticsByTable) {
    if (statisticsByTable == null) {
      return 0;
    }
    return statisticsByTable.values().stream().mapToLong(this::countStatistics).sum();
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
