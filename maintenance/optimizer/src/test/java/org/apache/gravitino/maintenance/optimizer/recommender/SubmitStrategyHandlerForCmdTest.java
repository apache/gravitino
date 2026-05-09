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

import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyEvaluation;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandler;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandlerContext;

/** Simple handler used by submit-strategy-jobs CLI tests. */
public class SubmitStrategyHandlerForCmdTest implements StrategyHandler {
  private StrategyHandlerContext context;

  @Override
  public void initialize(StrategyHandlerContext context) {
    this.context = context;
  }

  @Override
  public String strategyType() {
    return StrategyProviderForCmdTest.STRATEGY_TYPE;
  }

  @Override
  public boolean shouldTrigger() {
    return true;
  }

  @Override
  public StrategyEvaluation evaluate() {
    JobExecutionContext executionContext =
        new JobExecutionContext() {
          @Override
          public NameIdentifier nameIdentifier() {
            return context.nameIdentifier();
          }

          @Override
          public Map<String, String> jobOptions() {
            return context.strategy().jobOptions();
          }

          @Override
          public String jobTemplateName() {
            return context.strategy().jobTemplateName();
          }
        };
    return new StrategyEvaluation() {
      @Override
      public long score() {
        return 88L;
      }

      @Override
      public Optional<JobExecutionContext> jobExecutionContext() {
        return Optional.of(executionContext);
      }
    };
  }
}
