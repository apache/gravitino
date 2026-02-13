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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestStrategyFiltering {

  @Test
  void testRecommendOnlyTablesWithStrategy() {
    NameIdentifier tableWithPolicy = NameIdentifier.of("db", "t_with_policy");
    NameIdentifier tableWithoutPolicy = NameIdentifier.of("db", "t_without_policy");

    StubStrategy compactionStrategy = new StubStrategy("strategy-1", "COMPACTION", new HashMap<>());
    StrategyProvider strategyProvider =
        new StubStrategyProvider(
            Map.of(
                tableWithPolicy, List.of(compactionStrategy),
                tableWithoutPolicy, List.of()),
            Map.of(compactionStrategy.name(), compactionStrategy));

    Map<String, List<NameIdentifier>> identifiersByStrategy =
        groupIdentifiersByStrategyName(
            List.of(tableWithPolicy, tableWithoutPolicy),
            strategyProvider,
            compactionStrategy.strategyType());

    Assertions.assertEquals(
        1,
        identifiersByStrategy.get(compactionStrategy.name()).size(),
        "Only table with policy should be grouped");
    Assertions.assertEquals(
        tableWithPolicy,
        identifiersByStrategy.get(compactionStrategy.name()).get(0),
        "Wrong table grouped");
  }

  private static Map<String, List<NameIdentifier>> groupIdentifiersByStrategyName(
      List<NameIdentifier> identifiers, StrategyProvider strategyProvider, String strategyType) {
    Map<String, List<NameIdentifier>> identifiersByStrategyName = new HashMap<>();
    for (NameIdentifier identifier : identifiers) {
      strategyProvider.strategies(identifier).stream()
          .filter(strategy -> strategy.strategyType().equals(strategyType))
          .forEach(
              strategy ->
                  identifiersByStrategyName
                      .computeIfAbsent(strategy.name(), key -> new ArrayList<>())
                      .add(identifier));
    }
    return identifiersByStrategyName;
  }

  private static final class StubStrategy implements Strategy {
    private final String name;
    private final String strategyType;
    private final Map<String, Object> content;

    StubStrategy(String name, String strategyType, Map<String, Object> content) {
      this.name = name;
      this.strategyType = strategyType;
      this.content = content;
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
      return content;
    }

    @Override
    public Map<String, String> jobOptions() {
      return Map.of();
    }

    @Override
    public String jobTemplateName() {
      return "template";
    }
  }

  private static final class StubStrategyProvider implements StrategyProvider {
    private final Map<NameIdentifier, List<Strategy>> strategiesByTable;
    private final Map<String, Strategy> strategiesByName;

    StubStrategyProvider(
        Map<NameIdentifier, List<Strategy>> strategiesByTable, Map<String, Strategy> strategies) {
      this.strategiesByTable = strategiesByTable;
      this.strategiesByName = strategies;
    }

    @Override
    public String name() {
      return "stub-strategy-provider";
    }

    @Override
    public void initialize(
        org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv optimizerEnv) {}

    @Override
    public List<Strategy> strategies(NameIdentifier nameIdentifier) {
      return strategiesByTable.getOrDefault(nameIdentifier, List.of());
    }

    @Override
    public Strategy strategy(String strategyName) {
      return strategiesByName.get(strategyName);
    }

    @Override
    public void close() throws Exception {}
  }
}
