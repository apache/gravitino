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

import java.util.List;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.DataScope;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsProvider;

/** Handles CLI command {@code list-job-metrics}. */
public class ListJobMetricsCommand implements OptimizerCommandExecutor {

  @Override
  public void execute(OptimizerCommandContext context) {
    try (MetricsProvider metricsProvider =
        OptimizerCommandUtils.createMetricsProvider(context.optimizerEnv())) {
      for (NameIdentifier identifier : context.parsedIdentifiers()) {
        List<MetricPoint> metrics =
            metricsProvider.jobMetrics(
                identifier,
                OptimizerCommandUtils.METRICS_LIST_FROM_SECONDS,
                OptimizerCommandUtils.METRICS_LIST_TO_SECONDS);
        OptimizerOutputPrinter.printMetricsResult(
            context.output(), DataScope.Type.JOB, identifier, Optional.empty(), metrics);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to list job metrics", e);
    }
  }
}
