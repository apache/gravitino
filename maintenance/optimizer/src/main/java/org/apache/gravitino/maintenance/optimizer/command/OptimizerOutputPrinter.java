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

package org.apache.gravitino.maintenance.optimizer.command;

import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.DataScope;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.monitor.EvaluationResult;
import org.apache.gravitino.maintenance.optimizer.recommender.Recommender.RecommendationResult;
import org.apache.gravitino.maintenance.optimizer.updater.Updater.UpdateSummary;

/** Prints command output in stable plain-text format for optimizer CLI. */
final class OptimizerOutputPrinter {
  private OptimizerOutputPrinter() {}

  static void printEvaluationResult(PrintStream out, EvaluationResult result) {
    DataScope scope = result.scope();
    String partition =
        scope.partition().map(PartitionPath::toString).orElse("<table-or-job-scope>");
    out.printf(
        "EvaluationResult{scopeType=%s, identifier=%s, partitionPath=%s, evaluation=%s, "
            + "evaluatorName=%s, actionTimeSeconds=%d, rangeSeconds=%d, beforeMetrics=%s, "
            + "afterMetrics=%s}%n",
        scope.type(),
        scope.identifier(),
        partition,
        result.evaluation(),
        result.evaluatorName(),
        result.actionTimeSeconds(),
        result.rangeSeconds(),
        result.beforeMetrics(),
        result.afterMetrics());
  }

  static void printMetricsResult(
      PrintStream out,
      DataScope.Type scopeType,
      NameIdentifier identifier,
      Optional<PartitionPath> partitionPath,
      List<MetricPoint> metrics) {
    String partition = partitionPath.map(PartitionPath::toString).orElse("<table-or-job-scope>");
    out.printf(
        "MetricsResult{scopeType=%s, identifier=%s, partitionPath=%s, metrics=%s}%n",
        scopeType, identifier, partition, formatMetrics(metrics));
  }

  static void printUpdateSummary(PrintStream out, UpdateSummary summary) {
    out.printf(
        "SUMMARY: %s totalRecords=%d tableRecords=%d partitionRecords=%d jobRecords=%d%n",
        summary.updateType().name().toLowerCase(Locale.ROOT),
        summary.totalRecords(),
        summary.tableRecords(),
        summary.partitionRecords(),
        summary.jobRecords());
  }

  static void printSubmitResult(PrintStream out, RecommendationResult result) {
    out.printf(
        "SUBMIT: strategy=%s identifier=%s score=%d jobTemplate=%s jobOptions=%s jobId=%s%n",
        result.strategyName(),
        result.identifier(),
        result.score(),
        result.jobTemplate(),
        result.jobOptions(),
        result.jobId());
  }

  static void printDryRunResult(PrintStream out, RecommendationResult result) {
    out.printf(
        "DRY-RUN: strategy=%s identifier=%s score=%d jobTemplate=%s jobOptions=%s%n",
        result.strategyName(),
        result.identifier(),
        result.score(),
        result.jobTemplate(),
        result.jobOptions());
  }

  private static String formatMetrics(List<MetricPoint> metrics) {
    if (metrics == null || metrics.isEmpty()) {
      return "{}";
    }
    Map<String, List<MetricPoint>> metricsByName =
        metrics.stream()
            .collect(Collectors.groupingBy(MetricPoint::metricName, Collectors.toList()));
    return metricsByName.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(
            entry ->
                entry.getKey()
                    + "="
                    + formatMetricSamples(entry.getValue() == null ? List.of() : entry.getValue()))
        .collect(Collectors.joining(", ", "{", "}"));
  }

  private static String formatMetricSamples(List<MetricPoint> samples) {
    if (samples.isEmpty()) {
      return "[]";
    }
    return samples.stream()
        .sorted(Comparator.comparingLong(MetricPoint::timestampSeconds))
        .map(
            sample ->
                String.format(
                    "{timestamp=%d, value=%s}",
                    sample.timestampSeconds(), String.valueOf(sample.value().value())))
        .collect(Collectors.joining(", ", "[", "]"));
  }
}
