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

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyEvaluation;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandlerContext;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.recommender.strategy.GravitinoStrategy;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestGravitinoPolicyCompactionStrategy {

  @Test
  void testPolicyGeneratesExpectedStrategyAndJobContext() {
    PolicyContent content =
        PolicyContents.icebergCompaction(
            1000L,
            1L,
            2L,
            10L,
            Map.of("target-file-size-bytes", "1048576", "min-input-files", "4"));
    Policy policy = Mockito.mock(Policy.class);
    Mockito.when(policy.name()).thenReturn("iceberg-compaction-policy");
    Mockito.when(policy.content()).thenReturn(content);

    GravitinoStrategy strategy = new GravitinoStrategy(policy);
    Assertions.assertEquals(CompactionStrategyHandler.NAME, strategy.strategyType());
    Assertions.assertEquals("builtin-iceberg-rewrite-data-files", strategy.jobTemplateName());
    Assertions.assertEquals(
        Map.of("target-file-size-bytes", "1048576", "min-input-files", "4"), strategy.jobOptions());

    NameIdentifier tableId = NameIdentifier.of("db", "table");
    Table tableMetadata = Mockito.mock(Table.class);
    Mockito.when(tableMetadata.partitioning()).thenReturn(new Transform[0]);
    Mockito.when(tableMetadata.columns()).thenReturn(new Column[0]);
    List<StatisticEntry<?>> tableStatistics =
        List.of(
            new StatisticEntryImpl("custom-data-file-mse", StatisticValues.longValue(3000L)),
            new StatisticEntryImpl("custom-delete-file-number", StatisticValues.longValue(5L)));

    StrategyHandlerContext context =
        StrategyHandlerContext.builder(tableId, strategy)
            .withTableMetadata(tableMetadata)
            .withTableStatistics(tableStatistics)
            .build();

    CompactionStrategyHandler handler = new CompactionStrategyHandler();
    handler.initialize(context);

    Assertions.assertTrue(handler.shouldTrigger());

    StrategyEvaluation evaluation = handler.evaluate();
    Assertions.assertEquals(110L, evaluation.score());
    CompactionJobContext jobContext =
        (CompactionJobContext) evaluation.jobExecutionContext().orElseThrow();
    Assertions.assertEquals(tableId, jobContext.nameIdentifier());
    Assertions.assertEquals("builtin-iceberg-rewrite-data-files", jobContext.jobTemplateName());
    Assertions.assertEquals(
        Map.of("target-file-size-bytes", "1048576", "min-input-files", "4"),
        jobContext.jobOptions());
    Assertions.assertTrue(jobContext.getPartitions().isEmpty());
  }
}
