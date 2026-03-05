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

import org.apache.gravitino.maintenance.optimizer.updater.UpdateType;
import org.apache.gravitino.maintenance.optimizer.updater.Updater;
import org.apache.gravitino.maintenance.optimizer.updater.Updater.UpdateSummary;

/** Handles CLI command {@code append-metrics}. */
public class AppendMetricsCommand implements OptimizerCommandExecutor {

  @Override
  public void execute(OptimizerCommandContext context) throws Exception {
    try (Updater updater =
        new Updater(
            OptimizerCommandUtils.withStatisticsInput(
                context.optimizerEnv(), context.statisticsInputContent()))) {
      UpdateSummary summary;
      if (!context.hasIdentifiers()) {
        summary = updater.updateAll(context.calculatorName(), UpdateType.METRICS);
      } else {
        summary =
            updater.update(
                context.calculatorName(), context.parsedIdentifiers(), UpdateType.METRICS);
      }
      OptimizerOutputPrinter.printUpdateSummary(context.output(), summary);
    }
  }
}
