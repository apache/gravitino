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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StatisticsProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyEvaluation;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandler;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandlerContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.TableMetadataProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestRecommenderExecutionMode {
  private static final String STRATEGY_TYPE = "COMPACTION";

  @Test
  void testRecommendForStrategyTypeDoesNotSubmitJobs() {
    NameIdentifier identifier = NameIdentifier.of("catalog", "db", "table");
    Strategy strategyOne = new TestStrategy("s1", STRATEGY_TYPE, "tpl-1");
    Strategy strategyTwo = new TestStrategy("s2", STRATEGY_TYPE, "tpl-2");

    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    Mockito.when(strategyProvider.strategies(identifier))
        .thenReturn(List.of(strategyOne, strategyTwo));
    Mockito.when(strategyProvider.strategy("s1")).thenReturn(strategyOne);
    Mockito.when(strategyProvider.strategy("s2")).thenReturn(strategyTwo);
    JobSubmitter jobSubmitter = Mockito.mock(JobSubmitter.class);

    Recommender recommender =
        createRecommender(
            strategyProvider,
            Mockito.mock(StatisticsProvider.class),
            Mockito.mock(TableMetadataProvider.class),
            jobSubmitter);
    List<Recommender.RecommendationResult> results =
        recommender.recommendForStrategyType(List.of(identifier), STRATEGY_TYPE);

    Assertions.assertEquals(2, results.size());
    Assertions.assertTrue(results.stream().allMatch(result -> result.jobId().isEmpty()));
    Mockito.verify(jobSubmitter, Mockito.never())
        .submitJob(Mockito.anyString(), Mockito.any(JobExecutionContext.class));
  }

  @Test
  void testSubmitForStrategyNameSubmitsOnlySelectedStrategy() {
    NameIdentifier identifier = NameIdentifier.of("catalog", "db", "table");
    Strategy strategyOne = new TestStrategy("s1", STRATEGY_TYPE, "tpl-1");
    Strategy strategyTwo = new TestStrategy("s2", STRATEGY_TYPE, "tpl-2");

    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    Mockito.when(strategyProvider.strategies(identifier))
        .thenReturn(List.of(strategyOne, strategyTwo));
    Mockito.when(strategyProvider.strategy("s1")).thenReturn(strategyOne);
    Mockito.when(strategyProvider.strategy("s2")).thenReturn(strategyTwo);

    JobSubmitter jobSubmitter = Mockito.mock(JobSubmitter.class);
    Mockito.when(
            jobSubmitter.submitJob(Mockito.anyString(), Mockito.any(JobExecutionContext.class)))
        .thenReturn("job-1");

    Recommender recommender =
        createRecommender(
            strategyProvider,
            Mockito.mock(StatisticsProvider.class),
            Mockito.mock(TableMetadataProvider.class),
            jobSubmitter);
    List<Recommender.RecommendationResult> results =
        recommender.submitForStrategyName(List.of(identifier), "s1");

    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals("s1", results.get(0).strategyName());
    Assertions.assertEquals("job-1", results.get(0).jobId());
    Mockito.verify(jobSubmitter, Mockito.times(1))
        .submitJob(Mockito.eq("tpl-1"), Mockito.any(JobExecutionContext.class));
    Mockito.verify(jobSubmitter, Mockito.never())
        .submitJob(Mockito.eq("tpl-2"), Mockito.any(JobExecutionContext.class));
  }

  @Test
  void testSubmitForStrategyNameFailsWhenNoIdentifierMatches() {
    NameIdentifier identifier = NameIdentifier.of("catalog", "db", "table");
    Strategy strategy = new TestStrategy("s1", STRATEGY_TYPE, "tpl-1");

    StrategyProvider strategyProvider = Mockito.mock(StrategyProvider.class);
    Mockito.when(strategyProvider.strategies(identifier)).thenReturn(List.of(strategy));

    Recommender recommender =
        createRecommender(
            strategyProvider,
            Mockito.mock(StatisticsProvider.class),
            Mockito.mock(TableMetadataProvider.class),
            Mockito.mock(JobSubmitter.class));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> recommender.submitForStrategyName(List.of(identifier), "not-exist-strategy"));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("No identifiers matched strategy name 'not-exist-strategy'"));
  }

  private static Recommender createRecommender(
      StrategyProvider strategyProvider,
      StatisticsProvider statisticsProvider,
      TableMetadataProvider tableMetadataProvider,
      JobSubmitter jobSubmitter) {
    OptimizerConfig config = Mockito.mock(OptimizerConfig.class);
    Mockito.when(config.getStrategyHandlerClassName(STRATEGY_TYPE))
        .thenReturn(TestStrategyHandler.class.getName());
    OptimizerEnv optimizerEnv = Mockito.mock(OptimizerEnv.class);
    Mockito.when(optimizerEnv.config()).thenReturn(config);
    return new Recommender(
        strategyProvider, statisticsProvider, tableMetadataProvider, jobSubmitter, optimizerEnv);
  }

  public static class TestStrategyHandler implements StrategyHandler {
    private StrategyHandlerContext context;

    @Override
    public void initialize(StrategyHandlerContext context) {
      this.context = context;
    }

    @Override
    public String strategyType() {
      return STRATEGY_TYPE;
    }

    @Override
    public boolean shouldTrigger() {
      return true;
    }

    @Override
    public StrategyEvaluation evaluate() {
      Strategy strategy = context.strategy();
      JobExecutionContext jobExecutionContext =
          new JobExecutionContext() {
            @Override
            public NameIdentifier nameIdentifier() {
              return context.nameIdentifier();
            }

            @Override
            public Map<String, String> jobOptions() {
              return strategy.jobOptions();
            }

            @Override
            public String jobTemplateName() {
              return strategy.jobTemplateName();
            }
          };
      return new StrategyEvaluation() {
        @Override
        public long score() {
          return 100L;
        }

        @Override
        public Optional<JobExecutionContext> jobExecutionContext() {
          return Optional.of(jobExecutionContext);
        }
      };
    }
  }

  private static final class TestStrategy implements Strategy {
    private final String name;
    private final String strategyType;
    private final String templateName;

    private TestStrategy(String name, String strategyType, String templateName) {
      this.name = name;
      this.strategyType = strategyType;
      this.templateName = templateName;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String strategyType() {
      return strategyType;
    }

    @Override
    public Map<String, String> properties() {
      return Map.of();
    }

    @Override
    public Map<String, Object> rules() {
      return Map.of();
    }

    @Override
    public Map<String, String> jobOptions() {
      return Map.of("opt", "v");
    }

    @Override
    public String jobTemplateName() {
      return templateName;
    }
  }
}
