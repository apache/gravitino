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
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.monitor.EvaluationResult;
import org.apache.gravitino.maintenance.optimizer.monitor.Monitor;

/** Handles CLI command {@code monitor-metrics}. */
public class MonitorMetricsCommand implements OptimizerCommandExecutor {

  @Override
  public void execute(OptimizerCommandContext context) throws Exception {
    long actionTimeSeconds =
        OptimizerCommandUtils.parseLongOption("action-time", context.actionTime(), false);
    long rangeSeconds =
        OptimizerCommandUtils.parseLongOption("range-seconds", context.rangeSeconds(), true);
    Optional<PartitionPath> partitionPath =
        OptimizerCommandUtils.parsePartitionPath(context.partitionPathRaw());
    Preconditions.checkArgument(
        partitionPath.isEmpty() || context.identifiers().length == 1,
        "--partition-path requires exactly one identifier");
    try (Monitor monitor = new Monitor(context.optimizerEnv())) {
      for (NameIdentifier identifier : context.parsedIdentifiers()) {
        List<EvaluationResult> results =
            monitor.evaluateMetrics(identifier, actionTimeSeconds, rangeSeconds, partitionPath);
        results.forEach(
            result -> OptimizerOutputPrinter.printEvaluationResult(context.output(), result));
      }
    }
  }
}
