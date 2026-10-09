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

import com.google.common.base.Preconditions;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Evaluates a complete manifest measurement for exactly one resolved partition spec. */
public final class ManifestRewriteStrategyHandler implements StrategyHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ManifestRewriteStrategyHandler.class);

  private StrategyEvaluation evaluation = StrategyEvaluation.NO_EXECUTION;

  @Override
  public Set<DataRequirement> dataRequirements() {
    return Set.of(DataRequirement.TABLE_STATISTICS);
  }

  @Override
  public String strategyType() {
    return IcebergRewriteManifestsContent.STRATEGY_TYPE_VALUE;
  }

  @Override
  public void initialize(StrategyHandlerContext context) {
    evaluation = StrategyEvaluation.NO_EXECUTION;
    Integer requestedSpec = requestedSpec(context.strategy().rules());
    if (requestedSpec == null) {
      LOG.warn(
          "Skipping manifest rewrite policy {} for {}: set spec_id for persisted statistics "
              + "or pass the collector's resolved spec ID when initializing a collection cycle",
          context.strategy().name(),
          context.nameIdentifier());
      return;
    }
    initialize(context, requestedSpec);
  }

  /**
   * Evaluates statistics using the spec resolved at collection start. The default is never resolved
   * again, even if partition evolution has occurred since collection.
   *
   * @param context strategy and one consistent table-statistics response
   * @param resolvedSpecId spec returned by the collector
   */
  public void initialize(StrategyHandlerContext context, int resolvedSpecId) {
    evaluation = StrategyEvaluation.NO_EXECUTION;
    Objects.requireNonNull(context, "context");
    Preconditions.checkArgument(resolvedSpecId >= 0, "spec_id must be >= 0");
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
    Preconditions.checkArgument(
        requestedSpec == null || requestedSpec == resolvedSpecId,
        "Resolved spec ID does not match the policy spec_id");
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
    Optional<IcebergManifestStatistics> measurement =
        IcebergManifestStatistics.fromStatistics(statistics, resolvedSpecId);
    if (measurement.isEmpty()) {
      LOG.info(
          "Collect manifest statistics for spec {} of {} before evaluating",
          resolvedSpecId,
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
                  context.nameIdentifier(), resolvedSpecId, content.useCaching()));
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
    try {
      return Long.valueOf(value.toString());
    } catch (NumberFormatException e) {
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
