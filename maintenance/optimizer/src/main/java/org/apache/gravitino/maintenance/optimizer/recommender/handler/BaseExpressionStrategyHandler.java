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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.experimental.Accessors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionStrategy;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionStrategy.ScoreMode;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyEvaluation;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandler;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandlerContext;
import org.apache.gravitino.maintenance.optimizer.recommender.util.ExpressionEvaluator;
import org.apache.gravitino.maintenance.optimizer.recommender.util.QLExpressionEvaluator;
import org.apache.gravitino.maintenance.optimizer.recommender.util.StatisticsUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.util.StrategyUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.util.TableMetadataTriggerExpressionUtils;
import org.apache.gravitino.rel.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base strategy handler that provides common expression evaluation and statistics handling.
 *
 * <p>Subclasses supply strategy-specific logic while relying on the shared context, statistics
 * normalization, and expression evaluation utilities.
 */
public abstract class BaseExpressionStrategyHandler implements StrategyHandler {

  private static final Logger LOG = LoggerFactory.getLogger(BaseExpressionStrategyHandler.class);
  // Sort partitions by score descending (highest score first).
  private static final Comparator<PartitionScore> PARTITION_SCORE_ORDER =
      (a, b) -> Long.compare(b.score(), a.score());

  private final ExpressionEvaluator expressionEvaluator;
  private Strategy strategy;
  private List<StatisticEntry<?>> tableStatistics;
  private Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics;
  private Table tableMetadata;
  private NameIdentifier nameIdentifier;
  // Cached table-level context (table stats + metadata + rules), computed once per initialize().
  private Map<String, Object> tableLevelContext;

  /** Create a handler that evaluates expressions with the default QL evaluator. */
  protected BaseExpressionStrategyHandler() {
    this.expressionEvaluator = new QLExpressionEvaluator();
  }

  @Override
  public void initialize(StrategyHandlerContext context) {
    Preconditions.checkArgument(context.tableMetadata().isPresent(), "Table metadata is null");
    this.tableMetadata = context.tableMetadata().get();
    this.nameIdentifier = context.nameIdentifier();
    this.strategy = context.strategy();
    this.tableStatistics = context.tableStatistics();
    this.partitionStatistics = context.partitionStatistics();
    this.tableLevelContext = buildTableLevelContext();
  }

  @Override
  public boolean shouldTrigger() {
    if (isPartitionTable()) {
      return shouldTriggerForPartitionTable();
    }
    return shouldTriggerForNonPartitionTable();
  }

  @Override
  public StrategyEvaluation evaluate() {
    if (isPartitionTable()) {
      return evaluateForPartitionTable();
    }
    return evaluateForNonPartitionTable();
  }

  /**
   * Build the execution context for the selected partitions.
   *
   * @param nameIdentifier target table identifier
   * @param strategy strategy being evaluated
   * @param tableMetadata table metadata requested by the handler
   * @param partitions selected partitions, empty for non-partitioned tables
   * @param jobOptions job options derived from the strategy
   * @return job execution context
   */
  protected abstract JobExecutionContext buildJobExecutionContext(
      NameIdentifier nameIdentifier,
      Strategy strategy,
      Table tableMetadata,
      List<PartitionPath> partitions,
      Map<String, String> jobOptions);

  private int maxPartitionNum() {
    if (strategy instanceof PartitionStrategy) {
      return ((PartitionStrategy) strategy).maxPartitionNum();
    }
    return PartitionStrategy.DEFAULT_MAX_PARTITION_NUM;
  }

  private boolean isPartitionTable() {
    return tableMetadata.partitioning().length > 0;
  }

  private boolean shouldTriggerForPartitionTable() {
    if (partitionStatistics.isEmpty()) {
      LOG.info("No partition statistics available for table {}", nameIdentifier);
      return false;
    }
    String triggerExpression = triggerExpression(strategy);

    // Short-circuit: try to evaluate the trigger expression with table-level context only
    // (no partition statistics). This relies on the QL engine's left-to-right short-circuit
    // evaluation of && operators: if a table-level predicate appears first and evaluates to
    // false, the engine returns false without resolving subsequent partition-level variables.
    // Note: this optimization only works when the table-level predicate is positioned before the
    // partition-level predicate, e.g. "sort_order_count > 0 && datafile_mse < limit" will
    // short-circuit, but "datafile_mse < limit && sort_order_count > 0" will not.
    Optional<Boolean> evaluationResultWithoutPartitions =
        tryToEvaluateBool(triggerExpression, List.of());
    // If the trigger expression can be evaluated with table-level variables only, return the
    // result. Otherwise, evaluate the trigger expression for each partition.
    return evaluationResultWithoutPartitions.orElseGet(
        () ->
            partitionStatistics.values().stream()
                .anyMatch(partitionStats -> evaluateBool(triggerExpression, partitionStats)));
  }

  private boolean shouldTriggerForNonPartitionTable() {
    return evaluateBool(triggerExpression(strategy), List.of());
  }

  private StrategyEvaluation evaluateForNonPartitionTable() {
    long score = evaluateLong(scoreExpression(strategy), List.of());
    if (score <= 0) {
      return StrategyEvaluation.NO_EXECUTION;
    }
    JobExecutionContext jobContext =
        buildJobExecutionContext(
            nameIdentifier, strategy, tableMetadata, List.of(), strategy.jobOptions());
    return new StrategyEvaluationImpl(score, jobContext);
  }

  /**
   * Aggregate partition scores into a table score. The score mode is controlled by {@link
   * org.apache.gravitino.maintenance.optimizer.recommender.strategy.GravitinoStrategy#PARTITION_TABLE_SCORE_MODE}
   * and defaults to {@code avg}.
   */
  private long getTableScoreFromPartitions(List<PartitionScore> partitionScores) {
    if (partitionScores.isEmpty()) {
      return -1L;
    }
    ScoreMode scoreMode = partitionTableScoreMode();
    switch (scoreMode) {
      case SUM:
        return partitionScores.stream().mapToLong(PartitionScore::score).sum();
      case MAX:
        return partitionScores.stream().mapToLong(PartitionScore::score).max().orElse(-1L);
      case AVG:
        return partitionScores.stream().mapToLong(PartitionScore::score).sum()
            / partitionScores.size();
      default:
        LOG.warn(
            "Unsupported partition table score mode '{}' for strategy {}, defaulting to avg",
            scoreMode,
            strategy.name());
        return partitionScores.stream().mapToLong(PartitionScore::score).sum()
            / partitionScores.size();
    }
  }

  private StrategyEvaluation evaluateForPartitionTable() {
    List<PartitionScore> partitionScores = getTopPartitionScores(maxPartitionNum());
    if (partitionScores.isEmpty()) {
      return StrategyEvaluation.NO_EXECUTION;
    }
    List<PartitionPath> partitions =
        partitionScores.stream().map(PartitionScore::partition).collect(Collectors.toList());
    JobExecutionContext jobContext =
        buildJobExecutionContext(
            nameIdentifier, strategy, tableMetadata, partitions, strategy.jobOptions());
    long tableScore = getTableScoreFromPartitions(partitionScores);
    return new StrategyEvaluationImpl(tableScore, jobContext);
  }

  private ScoreMode partitionTableScoreMode() {
    if (strategy instanceof PartitionStrategy) {
      return ((PartitionStrategy) strategy).partitionTableScoreMode();
    }
    return ScoreMode.AVG;
  }

  private long evaluateLong(String expression, List<StatisticEntry<?>> statistics) {
    Map<String, Object> context = buildExpressionContext(statistics);
    try {
      return expressionEvaluator.evaluateLong(expression, context);
    } catch (RuntimeException e) {
      LOG.warn("Failed to evaluate expression '{}' with context {}", expression, context, e);
      return -1L;
    }
  }

  private boolean evaluateBool(String expression, List<StatisticEntry<?>> statistics) {
    Map<String, Object> context = buildExpressionContext(statistics);
    try {
      return expressionEvaluator.evaluateBool(expression, context);
    } catch (RuntimeException e) {
      LOG.warn("Failed to evaluate expression '{}' with context {}", expression, context, e);
      return false;
    }
  }

  private Optional<Boolean> tryToEvaluateBool(
      String expression, List<StatisticEntry<?>> statistics) {
    Map<String, Object> context = buildExpressionContext(statistics);
    try {
      return expressionEvaluator.tryToEvaluateBool(expression, context);
    } catch (RuntimeException e) {
      LOG.warn("Failed to evaluate expression '{}' with context {}", expression, context, e);
      // Per ExpressionEvaluator#tryToEvaluateBool, a failed evaluation yields an empty Optional so
      // the caller falls back to per-partition evaluation instead of treating it as "do not
      // trigger".
      return Optional.empty();
    }
  }

  /**
   * Build a combined evaluation context. The {@code statistics} argument varies per partition for
   * partitioned tables; it is layered on top of the precomputed {@link #tableLevelContext} (table
   * statistics, table metadata, and numeric rule values) so that {@code trigger-expr} and {@code
   * score-expr} can reference all of them.
   *
   * @param statistics statistics of the unit being evaluated (a single partition, or empty for the
   *     table-level evaluation)
   * @return combined context
   */
  private Map<String, Object> buildExpressionContext(List<StatisticEntry<?>> statistics) {
    Map<String, Object> context = new HashMap<>(tableLevelContext);
    context.putAll(StatisticsUtils.buildStatisticsContext(statistics));
    return context;
  }

  /** Precompute the table-level context shared across all partition evaluations. */
  private Map<String, Object> buildTableLevelContext() {
    Map<String, Object> context = new HashMap<>();
    context.putAll(StatisticsUtils.buildStatisticsContext(tableStatistics));
    context.putAll(TableMetadataTriggerExpressionUtils.buildTableMetadataContext(tableMetadata));
    context.putAll(getRulesExpressionContext(strategy));
    return context;
  }

  private static Map<String, Object> getRulesExpressionContext(Strategy strategy) {
    Map<String, Object> context = new HashMap<>();
    strategy
        .rules()
        .forEach(
            (k, v) -> {
              try {
                context.put(k, Long.parseLong(v.toString()));
              } catch (NumberFormatException e) {
                // Ignore non-numeric rule values when building numeric expression inputs.
              }
            });
    return context;
  }

  private String triggerExpression(Strategy strategy) {
    return StrategyUtils.getTriggerExpression(strategy);
  }

  private String scoreExpression(Strategy strategy) {
    return StrategyUtils.getScoreExpression(strategy);
  }

  /**
   * Return the highest-scoring partitions in descending order.
   *
   * @param limit max partitions to return
   * @return top partition scores, empty when none score above zero
   */
  private List<PartitionScore> getTopPartitionScores(int limit) {
    if (limit <= 0) {
      return List.of();
    }
    PriorityQueue<PartitionScore> scoreQueue =
        new PriorityQueue<>(limit, PARTITION_SCORE_ORDER.reversed());
    partitionStatistics.forEach(
        (partitionPath, statistics) -> {
          boolean trigger = evaluateBool(triggerExpression(strategy), statistics);
          if (trigger) {
            long partitionScore = evaluateLong(scoreExpression(strategy), statistics);
            if (partitionScore > 0) {
              PartitionScore entry = new PartitionScore(partitionPath, partitionScore);
              if (scoreQueue.size() < limit) {
                scoreQueue.add(entry);
              } else if (scoreQueue.peek() != null && partitionScore > scoreQueue.peek().score()) {
                scoreQueue.poll();
                scoreQueue.add(entry);
              }
            }
          }
        });

    return scoreQueue.stream().sorted(PARTITION_SCORE_ORDER).collect(Collectors.toList());
  }

  @Value
  @Accessors(fluent = true)
  private static final class PartitionScore {
    PartitionPath partition;
    long score;
  }
}
