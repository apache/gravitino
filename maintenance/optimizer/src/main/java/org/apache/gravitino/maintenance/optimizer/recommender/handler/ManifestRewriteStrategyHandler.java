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
package org.apache.gravitino.maintenance.optimizer.recommender.handler;

import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants.CURRENT_SNAPSHOT_ID;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants.DEFAULT_SPEC_ID;

import com.google.common.base.Preconditions;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyEvaluation;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandler;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandlerContext;
import org.apache.gravitino.maintenance.optimizer.common.IcebergManifestStatistics;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Evaluates a complete manifest measurement for exactly one resolved partition spec. */
public final class ManifestRewriteStrategyHandler implements StrategyHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ManifestRewriteStrategyHandler.class);

  private StrategyEvaluation evaluation = StrategyEvaluation.NO_EXECUTION;

  @Override
  public Set<DataRequirement> dataRequirements() {
    return Set.of(DataRequirement.TABLE_METADATA, DataRequirement.TABLE_STATISTICS);
  }

  @Override
  public String strategyType() {
    return IcebergRewriteManifestsContent.STRATEGY_TYPE_VALUE;
  }

  @Override
  public void initialize(StrategyHandlerContext context) {
    evaluation = StrategyEvaluation.NO_EXECUTION;
    Objects.requireNonNull(context, "context");
    Preconditions.checkArgument(
        strategyType().equals(context.strategy().strategyType()),
        "Unexpected strategy type: %s",
        context.strategy().strategyType());
    Preconditions.checkArgument(
        IcebergRewriteManifestsContent.JOB_TEMPLATE_NAME_VALUE.equals(
            context.strategy().jobTemplateName()),
        "Unexpected manifest rewrite job template");
    Map<String, Object> rules = context.strategy().rules();
    Integer requestedSpec = requestedSpec(rules);
    Integer resolvedSpec = requestedSpec == null ? defaultSpec(context) : requestedSpec;
    if (resolvedSpec == null) {
      LOG.warn(
          "Skipping manifest rewrite policy {} for {}: cannot resolve Iceberg {} from table metadata",
          context.strategy().name(),
          context.nameIdentifier(),
          DEFAULT_SPEC_ID);
      return;
    }
    IcebergRewriteManifestsContent content =
        PolicyContents.icebergRewriteManifests(
            longRule(rules, IcebergRewriteManifestsContent.MANIFEST_COUNT_CRITICAL),
            longRule(rules, IcebergRewriteManifestsContent.MANIFEST_COUNT_WARNING),
            longRule(rules, IcebergRewriteManifestsContent.AVG_MANIFEST_SIZE_THRESHOLD_BYTES),
            requestedSpec,
            booleanRule(rules, IcebergRewriteManifestsContent.USE_CACHING));
    content.validate();
    Map<String, StatisticValue<?>> statistics = new LinkedHashMap<>();
    for (StatisticEntry<?> entry : context.tableStatistics()) {
      Preconditions.checkArgument(
          statistics.putIfAbsent(entry.name(), entry.value()) == null,
          "Duplicate statistic: %s",
          entry.name());
    }
    if (!hasCurrentSnapshot(context, statistics.get(IcebergManifestStatistics.SNAPSHOT_ID))) {
      LOG.info(
          "Skipping manifest rewrite for {}: verify table snapshot metadata and recollect manifest statistics",
          context.nameIdentifier());
      return;
    }
    Optional<IcebergManifestStatistics> measurement =
        IcebergManifestStatistics.fromStatistics(statistics, resolvedSpec);
    if (measurement.isEmpty()) {
      LOG.info(
          "Collect manifest statistics for spec {} of {} before evaluating",
          resolvedSpec,
          context.nameIdentifier());
      return;
    }
    IcebergManifestStatistics values = measurement.get();
    if (values.count() >= content.manifestCountCritical()
        || (values.count() >= content.manifestCountWarning()
            && values.averageSize() < content.avgManifestSizeThresholdBytes())) {
      evaluation =
          new StrategyEvaluationImpl(
              values.count(),
              new ManifestRewriteJobContext(
                  context.nameIdentifier(), resolvedSpec, content.useCaching()));
    }
  }

  @Override
  public boolean shouldTrigger() {
    return evaluation.jobExecutionContext().isPresent();
  }

  @Override
  public StrategyEvaluation evaluate() {
    return evaluation;
  }

  private static boolean hasCurrentSnapshot(
      StrategyHandlerContext context, @Nullable StatisticValue<?> snapshot) {
    if (!(snapshot instanceof StatisticValues.LongValue)) {
      return false;
    }
    String currentSnapshot =
        context
            .tableMetadata()
            .map(table -> table.properties().get(CURRENT_SNAPSHOT_ID))
            .orElse(null);
    long collectedSnapshot = ((StatisticValues.LongValue) snapshot).value();
    return collectedSnapshot == -1L
        ? "none".equals(currentSnapshot)
        : collectedSnapshot >= 0 && Long.toString(collectedSnapshot).equals(currentSnapshot);
  }

  @Nullable
  private static Integer defaultSpec(StrategyHandlerContext context) {
    String value =
        context.tableMetadata().map(table -> table.properties().get(DEFAULT_SPEC_ID)).orElse(null);
    if (value == null || !value.matches("[0-9]+")) {
      return null;
    }
    try {
      return Integer.valueOf(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  @Nullable
  private static Integer requestedSpec(Map<String, Object> rules) {
    Long value = longRule(rules, IcebergRewriteManifestsContent.SPEC_ID);
    Preconditions.checkArgument(
        value == null || (value >= 0 && value <= Integer.MAX_VALUE),
        "spec_id must be a non-negative 32-bit integer");
    return value == null ? null : value.intValue();
  }

  @Nullable
  private static Long longRule(Map<String, Object> rules, String key) {
    Object value = rules.get(key);
    if (value == null) {
      return null;
    }
    if (value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long) {
      return ((Number) value).longValue();
    }
    try {
      if (value instanceof BigInteger) {
        return ((BigInteger) value).longValueExact();
      }
      Preconditions.checkArgument(
          value instanceof String && ((String) value).matches("-?[0-9]+"),
          "%s must be an integer",
          key);
      return Long.valueOf((String) value);
    } catch (NumberFormatException | ArithmeticException e) {
      throw new IllegalArgumentException(key + " must be an integer", e);
    }
  }

  @Nullable
  private static Boolean booleanRule(Map<String, Object> rules, String key) {
    Object value = rules.get(key);
    if (value == null) {
      return null;
    }
    Preconditions.checkArgument(
        "true".equalsIgnoreCase(value.toString()) || "false".equalsIgnoreCase(value.toString()),
        "%s must be true or false",
        key);
    return Boolean.valueOf(value.toString());
  }
}
