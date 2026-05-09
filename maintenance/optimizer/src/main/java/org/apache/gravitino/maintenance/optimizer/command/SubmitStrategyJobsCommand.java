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

package org.apache.gravitino.maintenance.optimizer.command;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.Recommender;
import org.apache.gravitino.maintenance.optimizer.recommender.Recommender.RecommendationResult;

/** Handles CLI command {@code submit-strategy-jobs}. */
public class SubmitStrategyJobsCommand implements OptimizerCommandExecutor {

  @Override
  public void execute(OptimizerCommandContext context) throws Exception {
    int limit = parseLimit(context.limit());
    try (Recommender recommender = new Recommender(context.optimizerEnv())) {
      List<RecommendationResult> results;
      if (context.dryRun()) {
        results =
            recommender.recommendForStrategyName(
                context.parsedIdentifiers(), context.strategyName(), limit);
        results.forEach(
            result -> OptimizerOutputPrinter.printDryRunResult(context.output(), result));
      } else {
        results =
            recommender.submitForStrategyName(
                context.parsedIdentifiers(), context.strategyName(), limit);
        results.forEach(
            result -> OptimizerOutputPrinter.printSubmitResult(context.output(), result));
      }
    }
  }

  private int parseLimit(String limit) {
    if (StringUtils.isBlank(limit)) {
      return Integer.MAX_VALUE;
    }

    long parsed = OptimizerCommandUtils.parseLongOption("limit", limit, false);
    Preconditions.checkArgument(
        parsed <= Integer.MAX_VALUE, "Option limit must be <= %s", Integer.MAX_VALUE);
    return (int) parsed;
  }
}
