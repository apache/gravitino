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

package org.apache.gravitino.maintenance.optimizer.recommender;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StatisticsProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyEvaluation;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandler;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandler.DataRequirement;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandlerContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.SupportTableStatistics;
import org.apache.gravitino.maintenance.optimizer.api.recommender.TableMetadataProvider;
import org.apache.gravitino.maintenance.optimizer.common.CloseableGroup;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.ProviderUtils;
import org.apache.gravitino.rel.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point that wires together strategy, statistics, metadata providers, and the job submitter
 * to generate optimization recommendations for tables.
 *
 * <p>Workflow:
 *
 * <ol>
 *   <li>Load {@link StrategyProvider}, {@link StatisticsProvider}, {@link TableMetadataProvider},
 *       and {@link JobSubmitter} from {@link java.util.ServiceLoader} using names declared in
 *       {@link OptimizerConfig}.
 *   <li>For each target identifier, fetch its strategies, filter by strategy type, and create a
 *       matching {@link StrategyHandler}.
 *   <li>Collect only the metadata and statistics the handler asked for via {@link
 *       StrategyHandler#dataRequirements()} and build a {@link StrategyHandlerContext}.
 *   <li>Invoke {@link StrategyHandler#shouldTrigger()} followed by {@link
 *       StrategyHandler#evaluate()} and optionally submit the resulting {@link JobExecutionContext}
 *       through the {@link JobSubmitter}.
 * </ol>
 */
public class Recommender implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(Recommender.class);
  private final StrategyProvider strategyProvider;
  private final StatisticsProvider statisticsProvider;
  private final TableMetadataProvider tableMetadataProvider;
  private final JobSubmitter jobSubmitter;
  private final CloseableGroup closeableGroup = new CloseableGroup();
  private final OptimizerEnv optimizerEnv;

  /**
   * Create a recommender whose providers and submitter are resolved from the optimizer
   * configuration. All components are initialized eagerly and closed together via {@link #close()}.
   *
   * @param optimizerEnv shared optimizer environment supplying configuration
   */
  public Recommender(OptimizerEnv optimizerEnv) {
    OptimizerConfig config = optimizerEnv.config();
    this.optimizerEnv = optimizerEnv;
    this.strategyProvider = loadStrategyProvider(config);
    this.strategyProvider.initialize(optimizerEnv);
    closeableGroup.register(strategyProvider, StrategyProvider.class.getSimpleName());

    this.statisticsProvider = loadStatisticsProvider(config);
    this.statisticsProvider.initialize(optimizerEnv);
    closeableGroup.register(statisticsProvider, StatisticsProvider.class.getSimpleName());

    this.tableMetadataProvider = loadTableMetadataProvider(config);
    this.tableMetadataProvider.initialize(optimizerEnv);
    closeableGroup.register(tableMetadataProvider, TableMetadataProvider.class.getSimpleName());

    this.jobSubmitter = loadJobSubmitter(config);
    this.jobSubmitter.initialize(optimizerEnv);
    closeableGroup.register(jobSubmitter, JobSubmitter.class.getSimpleName());
  }

  @VisibleForTesting
  Recommender(
      StrategyProvider strategyProvider,
      StatisticsProvider statisticsProvider,
      TableMetadataProvider tableMetadataProvider,
      JobSubmitter jobSubmitter,
      OptimizerEnv optimizerEnv) {

    this.optimizerEnv = optimizerEnv;
    this.strategyProvider = strategyProvider;
    this.statisticsProvider = statisticsProvider;
    this.tableMetadataProvider = tableMetadataProvider;
    this.jobSubmitter = jobSubmitter;

    addToCloseableGroup();
  }

  /**
   * Generate recommendations for all identifiers that have a strategy of the specified type.
   * Matching strategy instances are evaluated independently and returned for comparison. This
   * method does not submit any jobs.
   *
   * @param nameIdentifiers fully qualified table identifiers to evaluate (catalog/schema/table)
   * @param strategyType strategy type to filter and evaluate
   * @return recommendation results in execution order
   */
  public List<RecommendationResult> recommendForStrategyType(
      List<NameIdentifier> nameIdentifiers, String strategyType) {
    Preconditions.checkArgument(
        nameIdentifiers != null && !nameIdentifiers.isEmpty(), "nameIdentifiers must not be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(strategyType), "strategyType must not be blank");

    List<RecommendationResult> results = new ArrayList<>();
    Map<String, List<NameIdentifier>> identifiersByStrategyName =
        getIdentifiersByStrategyName(nameIdentifiers, strategyType);

    for (Map.Entry<String, List<NameIdentifier>> entry : identifiersByStrategyName.entrySet()) {
      String strategyName = entry.getKey();
      List<StrategyEvaluation> evaluations =
          recommendForOneStrategy(entry.getValue(), strategyName);
      for (StrategyEvaluation evaluation : evaluations) {
        results.add(toRecommendationResult(strategyName, evaluation, ""));
      }
    }
    return results;
  }

  /**
   * Evaluate and submit recommendations for one concrete strategy name.
   *
   * @param nameIdentifiers fully qualified table identifiers to evaluate (catalog/schema/table)
   * @param strategyName strategy name to evaluate and submit
   * @return submitted recommendation results in execution order
   */
  public List<RecommendationResult> submitForStrategyName(
      List<NameIdentifier> nameIdentifiers, String strategyName) {
    Preconditions.checkArgument(
        nameIdentifiers != null && !nameIdentifiers.isEmpty(), "nameIdentifiers must not be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(strategyName), "strategyName must not be blank");

    List<RecommendationResult> results = new ArrayList<>();
    List<NameIdentifier> identifiersForStrategy =
        getIdentifiersByExactStrategyName(nameIdentifiers, strategyName);
    List<StrategyEvaluation> evaluations =
        recommendForOneStrategy(identifiersForStrategy, strategyName);
    for (StrategyEvaluation evaluation : evaluations) {
      JobExecutionContext jobExecutionContext =
          evaluation
              .jobExecutionContext()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Job execution context is missing for evaluation of strategy "
                              + strategyName));
      String templateName = jobExecutionContext.jobTemplateName();
      String jobId = jobSubmitter.submitJob(templateName, jobExecutionContext);
      LOG.info(
          "Submit job {} for strategy {} with context {}",
          jobId,
          strategyName,
          jobExecutionContext);
      results.add(toRecommendationResult(strategyName, evaluation, jobId));
    }
    return results;
  }

  /** Close all registered providers and job submitter, suppressing secondary failures. */
  @Override
  public void close() throws Exception {
    closeableGroup.close();
  }

  private void addToCloseableGroup() {
    closeableGroup.register(strategyProvider, "strategy provider");
    closeableGroup.register(statisticsProvider, "statistics provider");
    closeableGroup.register(tableMetadataProvider, "table metadata provider");
    closeableGroup.register(jobSubmitter, "job submitter");
  }

  private List<StrategyEvaluation> recommendForOneStrategy(
      List<NameIdentifier> identifiers, String strategyName) {
    LOG.info("Recommend strategy {} for identifiers {}", strategyName, identifiers);
    Strategy strategy = strategyProvider.strategy(strategyName);

    PriorityQueue<StrategyEvaluation> scoreQueue =
        new PriorityQueue<>((a, b) -> Long.compare(b.score(), a.score()));
    for (NameIdentifier identifier : identifiers) {
      StrategyHandler strategyHandler = loadStrategyHandler(strategy, identifier);
      if (!strategyHandler.shouldTrigger()) {
        continue;
      }
      StrategyEvaluation evaluation = strategyHandler.evaluate();
      if (evaluation.score() < 0 || evaluation.jobExecutionContext().isEmpty()) {
        LOG.info(
            "Skip strategy {} for identifier {} because evaluation score is negative or job execution context is missing",
            strategyName,
            identifier);
        continue;
      }
      LOG.info(
          "Recommend strategy {} for identifier {} score: {}",
          strategyName,
          identifier,
          evaluation.score());
      scoreQueue.add(evaluation);
    }

    List<StrategyEvaluation> results = new ArrayList<>();
    while (!scoreQueue.isEmpty()) {
      results.add(scoreQueue.poll());
    }
    return results;
  }

  private StrategyHandler loadStrategyHandler(Strategy strategy, NameIdentifier nameIdentifier) {
    StrategyHandler strategyHandler = createStrategyHandler(strategy.strategyType());

    Set<DataRequirement> declaredRequirements = strategyHandler.dataRequirements();
    EnumSet<DataRequirement> requirements =
        declaredRequirements.isEmpty()
            ? EnumSet.noneOf(DataRequirement.class)
            : EnumSet.copyOf(declaredRequirements);
    Table tableMetadata = null;
    List<StatisticEntry<?>> statistics = Collections.emptyList();
    Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics = Collections.emptyMap();

    if (requirements.contains(DataRequirement.TABLE_METADATA)) {
      tableMetadata = tableMetadataProvider.tableMetadata(nameIdentifier);
    }

    if (requirements.contains(DataRequirement.TABLE_STATISTICS)
        || requirements.contains(DataRequirement.PARTITION_STATISTICS)) {
      SupportTableStatistics supportTableStatistics = requireTableStatisticsProvider();
      if (requirements.contains(DataRequirement.TABLE_STATISTICS)) {
        statistics = supportTableStatistics.tableStatistics(nameIdentifier);
      }
      if (requirements.contains(DataRequirement.PARTITION_STATISTICS)) {
        partitionStatistics = supportTableStatistics.partitionStatistics(nameIdentifier);
      }
    }

    StrategyHandlerContext context =
        StrategyHandlerContext.builder(nameIdentifier, strategy)
            .withTableMetadata(tableMetadata)
            .withTableStatistics(statistics)
            .withPartitionStatistics(partitionStatistics)
            .build();
    strategyHandler.initialize(context);

    return strategyHandler;
  }

  private StrategyHandler createStrategyHandler(String strategyType) {
    String strategyHandlerClassName = getStrategyHandlerClassName(strategyType);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(strategyHandlerClassName),
        "No StrategyHandler class configured for strategy type: %s",
        strategyType);
    try {
      return (StrategyHandler)
          Class.forName(strategyHandlerClassName).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      String message =
          "Failed to instantiate StrategyHandler '"
              + strategyHandlerClassName
              + "' for strategy type: "
              + strategyType;
      LOG.error(message, e);
      throw new RuntimeException(message, e);
    }
  }

  /**
   * Resolve the implementation class for a given strategy type. In production this should be backed
   * by configuration or an explicit registry that maps stable strategy type strings (for example,
   * {@code COMPACTION}) to {@link StrategyHandler} implementations.
   */
  private String getStrategyHandlerClassName(String strategyType) {
    return optimizerEnv.config().getStrategyHandlerClassName(strategyType);
  }

  private Map<String, List<NameIdentifier>> getIdentifiersByStrategyName(
      List<NameIdentifier> nameIdentifiers, String strategyType) {
    Map<String, List<NameIdentifier>> identifiersByStrategyName = new LinkedHashMap<>();
    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      strategyProvider.strategies(nameIdentifier).stream()
          .filter(strategy -> strategy.strategyType().equals(strategyType))
          .forEach(
              strategy ->
                  identifiersByStrategyName
                      .computeIfAbsent(strategy.name(), key -> new ArrayList<>())
                      .add(nameIdentifier));
    }
    return identifiersByStrategyName;
  }

  private List<NameIdentifier> getIdentifiersByExactStrategyName(
      List<NameIdentifier> nameIdentifiers, String strategyName) {
    List<NameIdentifier> identifiersForStrategy = new ArrayList<>();
    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      boolean matched =
          strategyProvider.strategies(nameIdentifier).stream()
              .anyMatch(strategy -> strategyName.equals(strategy.name()));
      if (matched) {
        identifiersForStrategy.add(nameIdentifier);
      }
    }
    return identifiersForStrategy;
  }

  private StrategyProvider loadStrategyProvider(OptimizerConfig config) {
    String strategyProviderName = config.get(OptimizerConfig.STRATEGY_PROVIDER_CONFIG);
    return ProviderUtils.createStrategyProviderInstance(strategyProviderName);
  }

  private StatisticsProvider loadStatisticsProvider(OptimizerConfig config) {
    String statisticsProviderName = config.get(OptimizerConfig.STATISTICS_PROVIDER_CONFIG);
    return ProviderUtils.createStatisticsProviderInstance(statisticsProviderName);
  }

  private TableMetadataProvider loadTableMetadataProvider(OptimizerConfig config) {
    String tableMetadataProviderName = config.get(OptimizerConfig.TABLE_META_PROVIDER_CONFIG);
    return ProviderUtils.createTableMetadataProviderInstance(tableMetadataProviderName);
  }

  private JobSubmitter loadJobSubmitter(OptimizerConfig config) {
    String jobSubmitterName = config.get(OptimizerConfig.JOB_SUBMITTER_CONFIG);
    return ProviderUtils.createJobSubmitterInstance(jobSubmitterName);
  }

  private SupportTableStatistics requireTableStatisticsProvider() {
    if (statisticsProvider instanceof SupportTableStatistics) {
      return (SupportTableStatistics) statisticsProvider;
    }
    throw new IllegalStateException(
        String.format(
            "Statistics provider '%s' does not implement SupportTableStatistics interface. "
                + "To use TABLE_STATISTICS or PARTITION_STATISTICS data requirements, "
                + "configure a statistics provider that implements SupportTableStatistics.",
            statisticsProvider.name()));
  }

  private RecommendationResult toRecommendationResult(
      String strategyName, StrategyEvaluation evaluation, String jobId) {
    JobExecutionContext jobExecutionContext =
        evaluation
            .jobExecutionContext()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Job execution context is missing for evaluation of strategy "
                            + strategyName));
    return new RecommendationResult(
        strategyName,
        jobExecutionContext.nameIdentifier(),
        evaluation.score(),
        jobExecutionContext.jobTemplateName(),
        jobExecutionContext.jobOptions(),
        jobId);
  }

  public static final class RecommendationResult {
    private final String strategyName;
    private final NameIdentifier identifier;
    private final long score;
    private final String jobTemplate;
    private final Map<String, String> jobOptions;
    private final String jobId;

    private RecommendationResult(
        String strategyName,
        NameIdentifier identifier,
        long score,
        String jobTemplate,
        Map<String, String> jobOptions,
        String jobId) {
      this.strategyName = strategyName;
      this.identifier = identifier;
      this.score = score;
      this.jobTemplate = jobTemplate;
      this.jobOptions = jobOptions;
      this.jobId = jobId;
    }

    public String strategyName() {
      return strategyName;
    }

    public NameIdentifier identifier() {
      return identifier;
    }

    public long score() {
      return score;
    }

    public String jobTemplate() {
      return jobTemplate;
    }

    public Map<String, String> jobOptions() {
      return jobOptions;
    }

    public String jobId() {
      return jobId;
    }
  }
}
