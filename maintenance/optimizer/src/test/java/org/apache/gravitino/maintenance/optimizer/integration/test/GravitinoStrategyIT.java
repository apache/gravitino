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
import java.util.List;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.maintenance.optimizer.recommender.strategy.GravitinoStrategyProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GravitinoStrategyIT extends AbstractGravitinoOptimizerEnvIT {

  private GravitinoStrategyProvider strategyProvider;

  @BeforeAll
  void init() {
    this.strategyProvider = new GravitinoStrategyProvider();
    strategyProvider.initialize(optimizerEnv);
  }

  @AfterAll
  void closeResources() throws Exception {
    if (strategyProvider != null) {
      strategyProvider.close();
    }
  }

  @Test
  void testGravitinoStrategyProviderGetStrategy() {
    createPolicy("test_policy", ImmutableMap.of("rule1", "value1"), "test");

    Strategy strategy = strategyProvider.strategy("test_policy");
    Assertions.assertNotNull(strategy);
    Assertions.assertEquals("test", strategy.strategyType());
    Assertions.assertEquals("template-name", strategy.jobTemplateName());
    Assertions.assertEquals(ImmutableMap.of("rule1", "value1"), strategy.rules());
  }

  @Test
  void testGravitinoStrategyProviderGetTableStrategies() {
    String tableName = "test_get_table_policy";
    createTable(tableName);
    createPolicy("policy1", ImmutableMap.of("rule1", "value1"), "test");
    createPolicy("policy2", ImmutableMap.of("rule2", "value2"), "test");
    associatePoliciesToTable("policy1", tableName);
    associatePoliciesToTable("policy2", tableName);

    List<Strategy> strategies = strategyProvider.strategies(getTableIdentifier(tableName));
    Assertions.assertNotNull(strategies);
    Assertions.assertEquals(2, strategies.size());

    strategies.stream()
        .forEach(
            strategy -> {
              if (strategy.name().equals("policy1")) {
                Assertions.assertEquals("test", strategy.strategyType());
                Assertions.assertEquals("template-name", strategy.jobTemplateName());
                Assertions.assertEquals(ImmutableMap.of("rule1", "value1"), strategy.rules());
              } else if (strategy.name().equals("policy2")) {
                Assertions.assertEquals("test", strategy.strategyType());
                Assertions.assertEquals("template-name", strategy.jobTemplateName());
                Assertions.assertEquals(ImmutableMap.of("rule2", "value2"), strategy.rules());
              } else {
                Assertions.fail("Unexpected strategy name: " + strategy.name());
              }
            });
  }
}
