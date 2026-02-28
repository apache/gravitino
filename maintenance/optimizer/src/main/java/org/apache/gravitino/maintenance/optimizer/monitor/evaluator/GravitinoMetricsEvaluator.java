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

package org.apache.gravitino.maintenance.optimizer.monitor.evaluator;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricScope;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsEvaluator;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Built-in {@link MetricsEvaluator} for comparing before/after metrics with configurable rules.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li>Set {@link OptimizerConfig#METRICS_EVALUATOR_CONFIG} to {@value #NAME}.
 *   <li>Configure {@link #EVALUATION_RULES_CONFIG} with comma-separated rule entries.
 * </ul>
 *
 * <p>Rule format:
 *
 * <ul>
 *   <li>{@code scope:metricName:aggregation:comparison}
 *   <li>{@code scope}: {@code table|job}
 *   <li>{@code aggregation}: {@code max|min|avg|latest}
 *   <li>{@code comparison}: {@code lt|le|gt|ge|eq|ne}
 * </ul>
 *
 * <p>Evaluation semantics:
 *
 * <ul>
 *   <li>Each rule selects one metric and computes aggregated {@code before} and {@code after}
 *       values.
 *   <li>The configured comparison is applied as {@code compare(after, before)}.
 *   <li>Partition scopes reuse table rules.
 *   <li>Rules with insufficient/non-aggregatable samples are skipped.
 *   <li>The overall result is pass when no rule fails.
 * </ul>
 *
 * <p>Example:
 *
 * <pre>{@code
 * gravitino.optimizer.monitor.metricsEvaluator = gravitino-metrics-evaluator
 * gravitino.optimizer.monitor.gravitinoMetricsEvaluator.rules =
 *   table:row_count:avg:le,job:duration:latest:le
 * }</pre>
 */
public class GravitinoMetricsEvaluator implements MetricsEvaluator {

  public static final String NAME = "gravitino-metrics-evaluator";
  public static final String CONFIG_NAME = "gravitinoMetricsEvaluator";

  /**
   * Comma-separated evaluation rules in format: {@code scope:metricName:aggregation:comparison}.
   * Supported scope values are {@code table|job}. Supported aggregation values are {@code
   * max|min|avg|latest}. Supported comparison values are {@code lt|le|gt|ge|eq|ne}. Table rules are
   * applied to both table and partition metric scopes.
   */
  public static final String EVALUATION_RULES_CONFIG =
      OptimizerConfig.MONITOR_PREFIX + CONFIG_NAME + ".rules";

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoMetricsEvaluator.class);

  private Map<RuleScope, Map<String, RuleConfig>> metricRulesByScope = Map.of();

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    String rawRules = optimizerEnv.config().getRawString(EVALUATION_RULES_CONFIG);
    this.metricRulesByScope = initRuleConfig(rawRules);
    if (metricRulesByScope.isEmpty()) {
      LOG.warn(
          "No evaluator rules configured in {}. All evaluations will pass until rules are set.",
          EVALUATION_RULES_CONFIG);
      return;
    }
    LOG.info(
        "Loaded {} scoped metric evaluation rules for {}: {}",
        ruleCount(metricRulesByScope),
        NAME,
        formatRules(metricRulesByScope));
  }

  /**
   * Parse evaluator rules from raw config text.
   *
   * <p>Rule format: {@code scope:metricName:aggregation:comparison}, separated by commas.
   *
   * @param rawRules raw rule text from config
   * @return immutable scoped rule map, or empty map when input is blank
   */
  private Map<RuleScope, Map<String, RuleConfig>> initRuleConfig(String rawRules) {
    if (StringUtils.isBlank(rawRules)) {
      return Map.of();
    }

    Map<RuleScope, Map<String, RuleConfig>> parsedRules = new LinkedHashMap<>();
    String[] entries = rawRules.split(",");
    for (String entry : entries) {
      String trimmed = entry == null ? "" : entry.trim();
      if (trimmed.isEmpty()) {
        continue;
      }

      String[] parts = trimmed.split(":", -1);
      if (parts.length != 4) {
        throw new IllegalArgumentException(
            "Invalid evaluator rule '"
                + trimmed
                + "'. Expected format is scope:metricName:aggregation:comparison");
      }

      String scopeText = parts[0].trim();
      String metricText = parts[1].trim();
      String aggregationText = parts[2].trim();
      String comparisonText = parts[3].trim();

      RuleScope scope = RuleScope.fromConfigValue(scopeText);
      String metricName = normalizeMetricName(metricText);
      if (metricName.isEmpty()) {
        throw new IllegalArgumentException("Metric name must not be blank in rule: " + trimmed);
      }
      AggregationOp aggregation = AggregationOp.from(aggregationText);
      ComparisonOp comparison = ComparisonOp.from(comparisonText);

      Map<String, RuleConfig> scopeRules =
          parsedRules.computeIfAbsent(scope, ignored -> new LinkedHashMap<>());
      RuleConfig previous =
          scopeRules.putIfAbsent(metricName, new RuleConfig(aggregation, comparison));
      if (previous != null) {
        throw new IllegalArgumentException(
            "Duplicate metric rule for '"
                + scope.configValue
                + ":"
                + metricName
                + "' in "
                + EVALUATION_RULES_CONFIG);
      }
    }

    if (parsedRules.isEmpty()) {
      return Map.of();
    }

    Map<RuleScope, Map<String, RuleConfig>> immutableRules = new LinkedHashMap<>();
    for (Map.Entry<RuleScope, Map<String, RuleConfig>> entry : parsedRules.entrySet()) {
      immutableRules.put(entry.getKey(), Collections.unmodifiableMap(entry.getValue()));
    }
    return Collections.unmodifiableMap(immutableRules);
  }

  @Override
  public boolean evaluateMetrics(
      MetricScope scope,
      Map<String, List<MetricSample>> beforeMetrics,
      Map<String, List<MetricSample>> afterMetrics) {
    if (metricRulesByScope.isEmpty()) {
      return true;
    }

    RuleScope ruleScope = RuleScope.fromMetricScopeType(scope.type());
    Map<String, RuleConfig> rules = metricRulesByScope.getOrDefault(ruleScope, Map.of());
    if (rules.isEmpty()) {
      LOG.info(
          "No evaluator rules configured for scope={}, identifier={}, skip evaluation",
          scope.type(),
          scope.identifier());
      return true;
    }

    Map<String, List<MetricSample>> before = beforeMetrics == null ? Map.of() : beforeMetrics;
    Map<String, List<MetricSample>> after = afterMetrics == null ? Map.of() : afterMetrics;

    int evaluatedCount = 0;
    int failedCount = 0;
    int skippedCount = 0;
    for (Map.Entry<String, RuleConfig> rule : rules.entrySet()) {
      evaluatedCount++;
      String metricName = rule.getKey();
      RuleConfig ruleConfig = rule.getValue();
      AggregationOp aggregation = ruleConfig.aggregation;
      ComparisonOp comparison = ruleConfig.comparison;

      List<MetricSample> beforeSamples = sanitizeSamples(findMetricSamples(before, metricName));
      List<MetricSample> afterSamples = sanitizeSamples(findMetricSamples(after, metricName));
      if (beforeSamples.isEmpty() || afterSamples.isEmpty()) {
        LOG.debug(
            "Metric {} of {} ({}) has insufficient samples: beforeSize={}, afterSize={}",
            metricName,
            scope.identifier(),
            scope.type(),
            beforeSamples.size(),
            afterSamples.size());
        skippedCount++;
        continue;
      }

      OptionalDouble beforeValue = aggregate(beforeSamples, aggregation);
      OptionalDouble afterValue = aggregate(afterSamples, aggregation);
      if (beforeValue.isEmpty() || afterValue.isEmpty()) {
        LOG.debug(
            "Metric {} of {} ({}) cannot be aggregated by {}",
            metricName,
            scope.identifier(),
            scope.type(),
            aggregation.configValue);
        skippedCount++;
        continue;
      }

      double beforeNumber = beforeValue.getAsDouble();
      double afterNumber = afterValue.getAsDouble();
      boolean passed = comparison.test(afterNumber, beforeNumber);
      LOG.debug(
          "Metric {} of {} ({}) using {}:{}: before={}, after={}, passed={}",
          metricName,
          scope.identifier(),
          scope.type(),
          aggregation.configValue,
          comparison.configValue,
          beforeNumber,
          afterNumber,
          passed);
      if (!passed) {
        failedCount++;
      }
    }

    boolean allPassed = failedCount == 0;
    LOG.info(
        "Evaluated {} metrics for {} ({}), passed={}, failed={}, skipped={}",
        evaluatedCount,
        scope.identifier(),
        scope.type(),
        allPassed,
        failedCount,
        skippedCount);
    return allPassed;
  }

  private static int ruleCount(Map<RuleScope, Map<String, RuleConfig>> rulesByScope) {
    return rulesByScope.values().stream().mapToInt(Map::size).sum();
  }

  private static String formatRules(Map<RuleScope, Map<String, RuleConfig>> rulesByScope) {
    return rulesByScope.entrySet().stream()
        .flatMap(
            scopeEntry ->
                scopeEntry.getValue().entrySet().stream()
                    .map(
                        metricEntry ->
                            scopeEntry.getKey().configValue
                                + ":"
                                + metricEntry.getKey()
                                + ":"
                                + metricEntry.getValue().aggregation.configValue
                                + ":"
                                + metricEntry.getValue().comparison.configValue))
        .collect(Collectors.joining(", "));
  }

  private static List<MetricSample> sanitizeSamples(List<MetricSample> samples) {
    return samples == null ? List.of() : samples;
  }

  private static List<MetricSample> findMetricSamples(
      Map<String, List<MetricSample>> metrics, String normalizedMetricName) {
    if (metrics == null || metrics.isEmpty()) {
      return List.of();
    }
    List<MetricSample> exactMatch = metrics.get(normalizedMetricName);
    if (exactMatch != null) {
      return exactMatch;
    }
    for (Map.Entry<String, List<MetricSample>> entry : metrics.entrySet()) {
      if (normalizeMetricName(entry.getKey()).equals(normalizedMetricName)) {
        return entry.getValue();
      }
    }
    return List.of();
  }

  private static String normalizeMetricName(String metricName) {
    return metricName == null ? "" : metricName.trim().toLowerCase(Locale.ROOT);
  }

  private static OptionalDouble aggregate(List<MetricSample> metrics, AggregationOp operation) {
    try {
      return switch (operation) {
        case MAX -> metrics.stream()
            .mapToDouble(GravitinoMetricsEvaluator::metricNumericValue)
            .max();
        case MIN -> metrics.stream()
            .mapToDouble(GravitinoMetricsEvaluator::metricNumericValue)
            .min();
        case AVG -> metrics.stream()
            .mapToDouble(GravitinoMetricsEvaluator::metricNumericValue)
            .average();
        case LATEST -> latest(metrics);
      };
    } catch (Exception e) {
      return OptionalDouble.empty();
    }
  }

  private static OptionalDouble latest(List<MetricSample> metrics) {
    Optional<MetricSample> latest =
        metrics.stream().max(Comparator.comparingLong(MetricSample::timestamp));
    return latest
        .map(sample -> OptionalDouble.of(metricNumericValue(sample)))
        .orElseGet(OptionalDouble::empty);
  }

  private static double metricNumericValue(MetricSample metric) {
    Object value = metric.statistic().value().value();
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    throw new IllegalArgumentException(
        "Metric value must be numeric, but got "
            + (value == null ? "null" : value.getClass().getName()));
  }

  private enum RuleScope {
    TABLE("table"),
    JOB("job");

    private final String configValue;

    RuleScope(String configValue) {
      this.configValue = configValue;
    }

    private static RuleScope fromConfigValue(String value) {
      String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
      for (RuleScope scope : values()) {
        if (scope.configValue.equals(normalized)) {
          return scope;
        }
      }
      throw new IllegalArgumentException(
          "Unsupported rule scope '"
              + value
              + "'. Supported scopes are: "
              + RuleScope.supportedValues());
    }

    private static RuleScope fromMetricScopeType(MetricScope.Type type) {
      return switch (type) {
        case TABLE -> TABLE;
        case PARTITION -> TABLE;
        case JOB -> JOB;
      };
    }

    private static String supportedValues() {
      return Arrays.stream(values())
          .map(scope -> scope.configValue)
          .collect(Collectors.joining("|"));
    }
  }

  private enum AggregationOp {
    MAX("max"),
    MIN("min"),
    AVG("avg"),
    LATEST("latest");

    private final String configValue;

    AggregationOp(String configValue) {
      this.configValue = configValue;
    }

    private static AggregationOp from(String value) {
      String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
      for (AggregationOp op : values()) {
        if (op.configValue.equals(normalized)) {
          return op;
        }
      }
      throw new IllegalArgumentException(
          "Unsupported aggregation operation '"
              + value
              + "'. Supported operations are: "
              + supportedValues());
    }

    private static String supportedValues() {
      return Arrays.stream(values()).map(op -> op.configValue).collect(Collectors.joining("|"));
    }
  }

  /**
   * Comparison operation used to verify aggregated {@code after} value against aggregated {@code
   * before} value.
   */
  private enum ComparisonOp {
    LT("lt"),
    LE("le"),
    GT("gt"),
    GE("ge"),
    EQ("eq"),
    NE("ne");

    private static final double EPSILON = 1e-9;
    private final String configValue;

    ComparisonOp(String configValue) {
      this.configValue = configValue;
    }

    /**
     * Apply this comparison to evaluated numbers.
     *
     * @param left evaluated "after" value
     * @param right evaluated "before" value
     * @return true if the comparison passes
     */
    private boolean test(double left, double right) {
      return switch (this) {
        case LT -> left < right;
        case LE -> left <= right;
        case GT -> left > right;
        case GE -> left >= right;
        case EQ -> Math.abs(left - right) <= EPSILON;
        case NE -> Math.abs(left - right) > EPSILON;
      };
    }

    private static ComparisonOp from(String value) {
      String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
      for (ComparisonOp op : values()) {
        if (op.configValue.equals(normalized)) {
          return op;
        }
      }
      throw new IllegalArgumentException(
          "Unsupported comparison operation '"
              + value
              + "'. Supported comparisons are: "
              + supportedValues());
    }

    private static String supportedValues() {
      return Arrays.stream(values()).map(op -> op.configValue).collect(Collectors.joining("|"));
    }
  }

  /** Parsed rule definition for one metric, including aggregation and comparison semantics. */
  private static class RuleConfig {
    private final AggregationOp aggregation;
    private final ComparisonOp comparison;

    private RuleConfig(AggregationOp aggregation, ComparisonOp comparison) {
      this.aggregation = aggregation;
      this.comparison = comparison;
    }

    @Override
    public String toString() {
      return aggregation.name() + ":" + comparison.name();
    }
  }
}
