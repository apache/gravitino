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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;

/** Test strategy provider used by {@code TestOptimizerCmd} for submit-strategy-jobs. */
public class StrategyProviderForCmdTest implements StrategyProvider {
  public static final String NAME = "strategy-provider-cmd-test";
  public static final String STRATEGY_NAME = "submit-strategy-cmd-test";
  public static final String STRATEGY_TYPE = "SUBMIT_TEST_TYPE";
  public static final String JOB_TEMPLATE = "noop-template";

  private static final NameIdentifier MATCHED_IDENTIFIER = NameIdentifier.of("test", "db", "table");
  private static final Strategy TEST_STRATEGY =
      new Strategy() {
        @Override
        public String name() {
          return STRATEGY_NAME;
        }

        @Override
        public String strategyType() {
          return STRATEGY_TYPE;
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
          return Map.of("k1", "v1");
        }

        @Override
        public String jobTemplateName() {
          return JOB_TEMPLATE;
        }
      };

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {}

  @Override
  public List<Strategy> strategies(NameIdentifier nameIdentifier) {
    return MATCHED_IDENTIFIER.equals(nameIdentifier) ? List.of(TEST_STRATEGY) : List.of();
  }

  @Override
  public Strategy strategy(String strategyName) {
    if (STRATEGY_NAME.equals(strategyName)) {
      return TEST_STRATEGY;
    }
    throw new IllegalArgumentException("Unknown strategy name: " + strategyName);
  }

  @Override
  public void close() throws Exception {}
}
