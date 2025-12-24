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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
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
 *       StrategyHandler#evaluate()} and hand the resulting {@link JobExecutionContext} to the
 *       {@link JobSubmitter}.
 * </ol>
 */
public class Recommender implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(Recommender.class);
  private final StrategyProvider strategyProvider;
  private final StatisticsProvider statisticsProvider;
  private final TableMetadataProvider tableMetadataProvider;
  private final JobSubmitter jobSubmitter;
  private final CloseableGroup closeableGroup = new CloseableGroup();

  /**
   * Create a recommender whose providers and submitter are resolved from the optimizer
   * configuration. All components are initialized eagerly and closed together via {@link #close()}.
   *
   * @param optimizerEnv shared optimizer environment supplying configuration
   */
  public Recommender(OptimizerEnv optimizerEnv) {
    OptimizerConfig config = optimizerEnv.config();
    StrategyProvider strategyProvider = loadStrategyProvider(config);
    StatisticsProvider statisticsProvider = loadStatisticsProvider(config);
    TableMetadataProvider tableMetadataProvider = loadTableMetadataProvider(config);
    JobSubmitter jobSubmitter = loadJobSubmitter(config);

    this.strategyProvider = strategyProvider;
    this.statisticsProvider = statisticsProvider;
    this.tableMetadataProvider = tableMetadataProvider;
    this.jobSubmitter = jobSubmitter;

    this.strategyProvider.initialize(optimizerEnv);
    this.statisticsProvider.initialize(optimizerEnv);
    this.tableMetadataProvider.initialize(optimizerEnv);
    this.jobSubmitter.initialize(optimizerEnv);

    closeableGroup.register(strategyProvider, "strategy provider");
    closeableGroup.register(statisticsProvider, "statistics provider");
    closeableGroup.register(tableMetadataProvider, "table metadata provider");
    closeableGroup.register(jobSubmitter, "job submitter");
  }

  /**
   * Generate and submit recommendations for all identifiers that have a strategy of the specified
   * type. Each matching strategy instance is evaluated independently and submitted through the
   * configured job submitter.
   *
   * @param nameIdentifiers fully qualified table identifiers to evaluate (catalog/schema/table)
   * @param strategyType strategy type to filter and evaluate
   */
  public void recommendForStrategyType(List<NameIdentifier> nameIdentifiers, String strategyType) {
    Preconditions.checkArgument(
        nameIdentifiers != null && !nameIdentifiers.isEmpty(), "nameIdentifiers must not be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(strategyType), "strategyType must not be blank");

    Map<String, List<NameIdentifier>> identifiersByStrategyName =
        getIdentifiersByStrategyName(nameIdentifiers, strategyType);

    for (Map.Entry<String, List<NameIdentifier>> entry : identifiersByStrategyName.entrySet()) {
      String strategyName = entry.getKey();
      List<JobExecutionContext> jobConfigs =
          recommendForOneStrategy(entry.getValue(), strategyName);
      for (JobExecutionContext jobConfig : jobConfigs) {
        String templateName = jobConfig.jobTemplateName();
        String jobId = jobSubmitter.submitJob(templateName, jobConfig);
        LOG.info("Submit job {} for strategy {} with context {}", jobId, strategyName, jobConfig);
      }
    }
  }

  /** Close all registered providers and job submitter, suppressing secondary failures. */
  @Override
  public void close() throws Exception {
    closeableGroup.close();
  }

  private List<JobExecutionContext> recommendForOneStrategy(
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
      LOG.info(
          "Recommend strategy {} for identifier {} score: {}",
          strategyName,
          identifier,
          evaluation.score());
      scoreQueue.add(evaluation);
    }

    return scoreQueue.stream()
        .map(StrategyEvaluation::jobExecutionContext)
        .collect(Collectors.toList());
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
  @SuppressWarnings("UnusedVariable")
  private String getStrategyHandlerClassName(String strategyType) {
    return "";
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
}
