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

package org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionStrategy;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionStrategy.ScoreMode;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyEvaluation;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandlerContext;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.recommender.strategy.GravitinoStrategy;
import org.apache.gravitino.maintenance.optimizer.recommender.util.StrategyUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestCompactionStrategyHandler {

  private final Strategy strategy = new CompactionStrategyForTest();

  @Test
  void testShouldTriggerWithoutPartition() {
    NameIdentifier tableId = NameIdentifier.of("db", "table");
    Table tableMetadata = Mockito.mock(Table.class);
    Mockito.when(tableMetadata.partitioning())
        .thenReturn(new org.apache.gravitino.rel.expressions.transforms.Transform[0]);

    List<StatisticEntry<?>> stats =
        Arrays.asList(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(2000L)));
    StrategyHandlerContext context =
        StrategyHandlerContext.builder(tableId, strategy)
            .withTableMetadata(tableMetadata)
            .withTableStatistics(stats)
            .build();
    CompactionStrategyHandler handler = new CompactionStrategyHandler();
    handler.initialize(context);
    Assertions.assertTrue(handler.shouldTrigger());

    stats = Arrays.asList(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(10L)));
    StrategyHandlerContext lowContext =
        StrategyHandlerContext.builder(tableId, strategy)
            .withTableMetadata(tableMetadata)
            .withTableStatistics(stats)
            .build();
    CompactionStrategyHandler lowHandler = new CompactionStrategyHandler();
    lowHandler.initialize(lowContext);
    Assertions.assertFalse(lowHandler.shouldTrigger());
  }

  @Test
  void testShouldTriggerActionWithPartitions() {
    NameIdentifier tableId = NameIdentifier.of("db", "table");
    Table tableMetadata = Mockito.mock(Table.class);
    Mockito.when(tableMetadata.partitioning())
        .thenReturn(
            new org.apache.gravitino.rel.expressions.transforms.Transform[] {
              Transforms.identity("p")
            });

    Map<PartitionPath, List<StatisticEntry<?>>> partitionStats =
        Map.of(
            PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "1"))),
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(10L))),
            PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "2"))),
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(2000L))));

    StrategyHandlerContext context =
        StrategyHandlerContext.builder(tableId, strategy)
            .withTableMetadata(tableMetadata)
            .withTableStatistics(Arrays.asList())
            .withPartitionStatistics(partitionStats)
            .build();

    CompactionStrategyHandler handler = new CompactionStrategyHandler();
    handler.initialize(context);
    Assertions.assertTrue(handler.shouldTrigger());

    Map<PartitionPath, List<StatisticEntry<?>>> allLowStats =
        Map.of(
            PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "1"))),
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(1L))),
            PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "2"))),
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(5L))));
    StrategyHandlerContext lowContext =
        StrategyHandlerContext.builder(tableId, strategy)
            .withTableMetadata(tableMetadata)
            .withTableStatistics(Arrays.asList())
            .withPartitionStatistics(allLowStats)
            .build();

    CompactionStrategyHandler lowHandler = new CompactionStrategyHandler();
    lowHandler.initialize(lowContext);
    Assertions.assertFalse(lowHandler.shouldTrigger());
  }

  @Test
  void testJobConfig() {
    NameIdentifier tableId = NameIdentifier.of("db", "table");
    Table tableMetadata = Mockito.mock(Table.class);
    JobExecutionContext config =
        new CompactionJobContext(
            tableId,
            strategy.jobOptions(),
            strategy.jobTemplateName(),
            tableMetadata.columns(),
            tableMetadata.partitioning(),
            List.of());
    Assertions.assertTrue(config instanceof CompactionJobContext);
    CompactionJobContext compactionConfig = (CompactionJobContext) config;
    Assertions.assertEquals(tableId, compactionConfig.nameIdentifier());
    Assertions.assertEquals(
        ImmutableMap.of(CompactionStrategyForTest.TARGET_FILE_SIZE_BYTES, "1024"),
        compactionConfig.jobOptions());
    Assertions.assertTrue(compactionConfig.getPartitions().isEmpty());
  }

  @Test
  void testEvaluatePartitionTableScoreMode() {
    NameIdentifier tableId = NameIdentifier.of("db", "table");
    Table tableMetadata = Mockito.mock(Table.class);
    Mockito.when(tableMetadata.partitioning())
        .thenReturn(
            new org.apache.gravitino.rel.expressions.transforms.Transform[] {
              Transforms.identity("p")
            });
    Mockito.when(tableMetadata.columns()).thenReturn(new Column[0]);

    Map<PartitionPath, List<StatisticEntry<?>>> partitionStats =
        Map.of(
            PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "1"))),
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(10L))),
            PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "2"))),
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(30L))));

    Assertions.assertEquals(
        40L, evaluatePartitionScore(tableId, tableMetadata, partitionStats, ScoreMode.SUM, null));
    Assertions.assertEquals(
        30L, evaluatePartitionScore(tableId, tableMetadata, partitionStats, ScoreMode.MAX, null));
    Assertions.assertEquals(
        20L, evaluatePartitionScore(tableId, tableMetadata, partitionStats, null, null));
  }

  @Test
  void testEvaluateMaxPartitionNumFromStrategy() {
    NameIdentifier tableId = NameIdentifier.of("db", "table");
    Table tableMetadata = Mockito.mock(Table.class);
    Mockito.when(tableMetadata.partitioning())
        .thenReturn(
            new org.apache.gravitino.rel.expressions.transforms.Transform[] {
              Transforms.identity("p")
            });
    Mockito.when(tableMetadata.columns()).thenReturn(new Column[0]);

    Map<PartitionPath, List<StatisticEntry<?>>> partitionStats =
        Map.of(
            PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "1"))),
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(10L))),
            PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "2"))),
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(20L))),
            PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "3"))),
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(30L))));

    Assertions.assertEquals(
        30L, evaluatePartitionScore(tableId, tableMetadata, partitionStats, null, 1));
    Assertions.assertEquals(
        25L, evaluatePartitionScore(tableId, tableMetadata, partitionStats, null, 2));
  }

  @Test
  void testTopPartitionSelectionRespectsLimitAndOrder() {
    NameIdentifier tableId = NameIdentifier.of("db", "table");
    Table tableMetadata = Mockito.mock(Table.class);
    Mockito.when(tableMetadata.partitioning())
        .thenReturn(
            new org.apache.gravitino.rel.expressions.transforms.Transform[] {
              Transforms.identity("p")
            });
    Mockito.when(tableMetadata.columns()).thenReturn(new Column[0]);

    PartitionPath lowPartition = PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "1")));
    PartitionPath highPartition = PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "2")));
    PartitionPath midPartition = PartitionPath.of(Arrays.asList(new PartitionEntryImpl("p", "3")));

    Map<PartitionPath, List<StatisticEntry<?>>> partitionStats =
        Map.of(
            lowPartition,
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(10L))),
            highPartition,
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(30L))),
            midPartition,
            List.of(new StatisticEntryImpl("datafile_mse", StatisticValues.longValue(20L))));

    StrategyHandlerContext context =
        StrategyHandlerContext.builder(tableId, buildStrategy(null, 2))
            .withTableMetadata(tableMetadata)
            .withTableStatistics(List.of())
            .withPartitionStatistics(partitionStats)
            .build();

    CompactionStrategyHandler handler = new CompactionStrategyHandler();
    handler.initialize(context);
    StrategyEvaluation evaluation = handler.evaluate();

    CompactionJobContext jobContext =
        (CompactionJobContext) evaluation.jobExecutionContext().orElseThrow();
    List<PartitionPath> selected = jobContext.getPartitions();

    Assertions.assertEquals(2, selected.size(), "Should return top two partitions");
    Assertions.assertEquals(highPartition, selected.get(0), "Highest score should come first");
    Assertions.assertEquals(midPartition, selected.get(1), "Second highest score expected");
  }

  private long evaluatePartitionScore(
      NameIdentifier tableId,
      Table tableMetadata,
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStats,
      ScoreMode scoreMode,
      Integer maxPartitionNum) {
    StrategyHandlerContext context =
        StrategyHandlerContext.builder(tableId, buildStrategy(scoreMode, maxPartitionNum))
            .withTableMetadata(tableMetadata)
            .withTableStatistics(List.of())
            .withPartitionStatistics(partitionStats)
            .build();

    CompactionStrategyHandler handler = new CompactionStrategyHandler();
    handler.initialize(context);
    StrategyEvaluation evaluation = handler.evaluate();
    Assertions.assertTrue(evaluation.jobExecutionContext().isPresent());
    return evaluation.score();
  }

  private Strategy buildStrategy(ScoreMode scoreMode, Integer maxPartitionNum) {
    Map<String, Object> rules = new HashMap<>();
    rules.put(StrategyUtils.TRIGGER_EXPR, "datafile_mse > 0");
    rules.put(StrategyUtils.SCORE_EXPR, "datafile_mse");
    if (scoreMode != null) {
      rules.put(GravitinoStrategy.PARTITION_TABLE_SCORE_MODE, scoreMode);
    }
    if (maxPartitionNum != null) {
      rules.put(GravitinoStrategy.MAX_PARTITION_NUM, maxPartitionNum);
    }
    return new PartitionStrategy() {
      @Override
      public String name() {
        return "compaction-score-mode-test";
      }

      @Override
      public String strategyType() {
        return "compaction";
      }

      @Override
      public Map<String, Object> rules() {
        return rules;
      }

      @Override
      public Map<String, String> properties() {
        return Map.of();
      }

      @Override
      public Map<String, String> jobOptions() {
        return Map.of();
      }

      @Override
      public String jobTemplateName() {
        return "compaction-template";
      }

      @Override
      public ScoreMode partitionTableScoreMode() {
        Object value = rules.get(GravitinoStrategy.PARTITION_TABLE_SCORE_MODE);
        return value instanceof ScoreMode ? (ScoreMode) value : ScoreMode.AVG;
      }

      @Override
      public int maxPartitionNum() {
        Object value = rules.get(GravitinoStrategy.MAX_PARTITION_NUM);
        if (value == null) {
          return 100;
        }
        if (value instanceof Number) {
          int parsed = ((Number) value).intValue();
          return parsed > 0 ? parsed : 100;
        }
        try {
          int parsed = Integer.parseInt(value.toString());
          return parsed > 0 ? parsed : 100;
        } catch (NumberFormatException e) {
          return 100;
        }
      }
    };
  }
}
