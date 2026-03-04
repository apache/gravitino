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

package org.apache.gravitino.maintenance.optimizer.integration.test;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyEvaluation;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsUpdater;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.recommender.Recommender;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionJobContext;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionStrategyHandler;
import org.apache.gravitino.maintenance.optimizer.recommender.util.StrategyUtils;
import org.apache.gravitino.maintenance.optimizer.updater.statistics.GravitinoStatisticsUpdater;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/*
 * 1. update table stats
 * 2. add policy
 * 3. run recommender to get optimize result
 */
public class RecommenderIT extends GravitinoOptimizerEnvIT {

  private static final String STATISTICS_PREFIX = "custom-";
  private static final String DATAFILE_MSE = STATISTICS_PREFIX + "datafile_size_mse";
  private static final String DELETE_FILE_NUM = STATISTICS_PREFIX + "position_delete_file_number";

  private StatisticsUpdater statisticsUpdater;

  @Override
  protected Map<String, String> getSpecifyConfigs() {
    return Map.of(
        OptimizerConfig.OPTIMIZER_PREFIX
            + "strategyHandler."
            + CompactionStrategyHandler.NAME
            + ".className",
        CompactionStrategyHandler.class.getName());
  }

  @BeforeAll
  void init() {
    this.statisticsUpdater = new GravitinoStatisticsUpdater();
    statisticsUpdater.initialize(optimizerEnv);
  }

  @Test
  void testRecommendNonPartitionTable() throws Exception {
    String policyForDelete = "policyForDelete";
    String policyForSmallFile = "policyForSmallFile";

    String tableWithSmallFile = "tableWithSmallFile";
    String tableWithDeleteFile = "tableWithDeleteFile";
    String tableWithoutCompaction = "tableWithoutCompaction";

    createTable(tableWithSmallFile);
    createTable(tableWithDeleteFile);
    createTable(tableWithoutCompaction);

    createPolicy(
        policyForSmallFile,
        ImmutableMap.of(
            "min_datafile_mse",
            1000,
            StrategyUtils.TRIGGER_EXPR,
            DATAFILE_MSE + " > min_datafile_mse || " + DELETE_FILE_NUM + " > 0",
            StrategyUtils.SCORE_EXPR,
            DATAFILE_MSE + " + " + DELETE_FILE_NUM + " * 10"),
        CompactionStrategyHandler.NAME);

    createPolicy(
        policyForDelete,
        ImmutableMap.of(
            "min_datafile_mse",
            1000,
            StrategyUtils.TRIGGER_EXPR,
            DATAFILE_MSE + " > min_datafile_mse || " + DELETE_FILE_NUM + " > 1",
            StrategyUtils.SCORE_EXPR,
            DATAFILE_MSE + "/100 + " + DELETE_FILE_NUM + " * 100"),
        CompactionStrategyHandler.NAME);

    associatePoliciesToSchema(policyForSmallFile, TEST_SCHEMA);
    associatePoliciesToSchema(policyForDelete, TEST_SCHEMA);

    statisticsUpdater.updateTableStatistics(
        getTableIdentifier(tableWithSmallFile),
        Arrays.asList(
            new StatisticEntryImpl<>(DELETE_FILE_NUM, StatisticValues.longValue(2)),
            new StatisticEntryImpl<>(DATAFILE_MSE, StatisticValues.doubleValue(10000.1))));

    statisticsUpdater.updateTableStatistics(
        getTableIdentifier(tableWithDeleteFile),
        Arrays.asList(
            new StatisticEntryImpl<>(DELETE_FILE_NUM, StatisticValues.longValue(100)),
            new StatisticEntryImpl<>(DATAFILE_MSE, StatisticValues.doubleValue(100.1))));

    statisticsUpdater.updateTableStatistics(
        getTableIdentifier(tableWithoutCompaction),
        Arrays.asList(
            new StatisticEntryImpl<>(DELETE_FILE_NUM, StatisticValues.longValue(0)),
            new StatisticEntryImpl<>(DATAFILE_MSE, StatisticValues.doubleValue(0))));

    try (Recommender recommender = new Recommender(optimizerEnv)) {
      List<JobExecutionContext> jobs =
          recommendForOneStrategy(
              recommender,
              Arrays.asList(
                  getTableIdentifier(tableWithSmallFile),
                  getTableIdentifier(tableWithDeleteFile),
                  getTableIdentifier(tableWithoutCompaction)),
              policyForSmallFile);
      Assertions.assertEquals(2, jobs.size());
      Assertions.assertEquals(tableWithSmallFile, jobs.get(0).nameIdentifier().name());
      Assertions.assertEquals(tableWithDeleteFile, jobs.get(1).nameIdentifier().name());

      jobs =
          recommendForOneStrategy(
              recommender,
              Arrays.asList(
                  getTableIdentifier(tableWithSmallFile),
                  getTableIdentifier(tableWithDeleteFile),
                  getTableIdentifier(tableWithoutCompaction)),
              policyForDelete);
      Assertions.assertEquals(2, jobs.size());
      Assertions.assertEquals(tableWithDeleteFile, jobs.get(0).nameIdentifier().name());
      Assertions.assertEquals(tableWithSmallFile, jobs.get(1).nameIdentifier().name());
    }
  }

  @Test
  void testCompactionPartitionTable() throws Exception {
    String tableName = "partitionTable";
    String tableName2 = "partitionTable2";
    String policyName = "policyForPartition";

    createPartitionTable(tableName);
    createPartitionTable(tableName2);

    createPolicy(
        policyName,
        ImmutableMap.of(
            StrategyUtils.TRIGGER_EXPR,
            "true",
            StrategyUtils.SCORE_EXPR,
            DELETE_FILE_NUM + " * 100 + " + DATAFILE_MSE),
        CompactionStrategyHandler.NAME);
    associatePoliciesToTable(policyName, tableName);

    List<PartitionEntry> partition1 =
        Arrays.asList(new PartitionEntryImpl("col1", "1"), new PartitionEntryImpl("col2", "3"));
    List<PartitionEntry> partition2 =
        Arrays.asList(new PartitionEntryImpl("col1", "2"), new PartitionEntryImpl("col2", "4"));
    List<PartitionEntry> partition3 =
        Arrays.asList(new PartitionEntryImpl("col1", "10"), new PartitionEntryImpl("col2", "5"));

    statisticsUpdater.updateTableStatistics(
        getTableIdentifier(tableName),
        Arrays.asList(
            new StatisticEntryImpl<>(DELETE_FILE_NUM, StatisticValues.longValue(1)),
            new StatisticEntryImpl<>(DATAFILE_MSE, StatisticValues.doubleValue(10.0))));
    statisticsUpdater.updatePartitionStatistics(
        getTableIdentifier(tableName),
        Map.of(
            PartitionPath.of(partition1),
            List.of(
                new StatisticEntryImpl<>(DELETE_FILE_NUM, StatisticValues.longValue(5)),
                new StatisticEntryImpl<>(DATAFILE_MSE, StatisticValues.doubleValue(1000.0))),
            PartitionPath.of(partition2),
            List.of(
                new StatisticEntryImpl<>(DELETE_FILE_NUM, StatisticValues.longValue(1)),
                new StatisticEntryImpl<>(DATAFILE_MSE, StatisticValues.doubleValue(100.0)))));

    statisticsUpdater.updateTableStatistics(
        getTableIdentifier(tableName2),
        Arrays.asList(
            new StatisticEntryImpl<>(DELETE_FILE_NUM, StatisticValues.longValue(1)),
            new StatisticEntryImpl<>(DATAFILE_MSE, StatisticValues.doubleValue(10.0))));
    statisticsUpdater.updatePartitionStatistics(
        getTableIdentifier(tableName2),
        Map.of(
            PartitionPath.of(partition3),
            List.of(
                new StatisticEntryImpl<>(DELETE_FILE_NUM, StatisticValues.longValue(2)),
                new StatisticEntryImpl<>(DATAFILE_MSE, StatisticValues.doubleValue(500.0)))));

    try (Recommender recommender = new Recommender(optimizerEnv)) {
      List<JobExecutionContext> jobs =
          recommendForOneStrategy(
              recommender,
              Arrays.asList(getTableIdentifier(tableName), getTableIdentifier(tableName2)),
              policyName);

      Assertions.assertEquals(2, jobs.size());
      jobs.forEach(job -> Assertions.assertTrue(job instanceof CompactionJobContext));

      Map<String, CompactionJobContext> jobByTable =
          jobs.stream()
              .map(job -> (CompactionJobContext) job)
              .collect(Collectors.toMap(ctx -> ctx.nameIdentifier().name(), Function.identity()));

      CompactionJobContext jobContext1 = jobByTable.get(tableName);
      Assertions.assertNotNull(jobContext1);
      Assertions.assertFalse(jobContext1.getPartitions().isEmpty());
      Assertions.assertEquals(2, jobContext1.getPartitions().size());
      Assertions.assertEquals(
          Arrays.asList("col1=1", "col2=3"),
          jobContext1.getPartitions().get(0).entries().stream()
              .map(p -> p.partitionName() + "=" + p.partitionValue())
              .toList());
      Assertions.assertEquals(
          Arrays.asList("col1=2", "col2=4"),
          jobContext1.getPartitions().get(1).entries().stream()
              .map(p -> p.partitionName() + "=" + p.partitionValue())
              .toList());

      CompactionJobContext jobContext2 = jobByTable.get(tableName2);
      Assertions.assertNotNull(jobContext2);
      Assertions.assertFalse(jobContext2.getPartitions().isEmpty());
      Assertions.assertEquals(1, jobContext2.getPartitions().size());
      Assertions.assertEquals(
          Arrays.asList("col1=10", "col2=5"),
          jobContext2.getPartitions().get(0).entries().stream()
              .map(p -> p.partitionName() + "=" + p.partitionValue())
              .toList());
    }
  }

  private List<JobExecutionContext> recommendForOneStrategy(
      Recommender recommender, List<NameIdentifier> identifiers, String strategyName) {
    try {
      Method method =
          Recommender.class.getDeclaredMethod("recommendForOneStrategy", List.class, String.class);
      method.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<StrategyEvaluation> evaluations =
          (List<StrategyEvaluation>) method.invoke(recommender, identifiers, strategyName);
      return evaluations.stream()
          .map(
              evaluation ->
                  evaluation
                      .jobExecutionContext()
                      .orElseThrow(
                          () ->
                              new IllegalStateException(
                                  String.format(
                                      "Job execution context is missing for strategy %s",
                                      strategyName))))
          .toList();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to invoke recommender internals", e);
    }
  }
}
