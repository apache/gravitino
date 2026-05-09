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
import org.apache.gravitino.maintenance.optimizer.api.common.DataScope;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsProvider;

/** Handles CLI command {@code list-table-metrics}. */
public class ListTableMetricsCommand implements OptimizerCommandExecutor {

  @Override
  public void execute(OptimizerCommandContext context) {
    Optional<PartitionPath> partitionPath =
        OptimizerCommandUtils.parsePartitionPath(context.partitionPathRaw());
    Preconditions.checkArgument(
        partitionPath.isEmpty() || context.identifiers().length == 1,
        "--partition-path requires exactly one identifier");
    try (MetricsProvider metricsProvider =
        OptimizerCommandUtils.createMetricsProvider(context.optimizerEnv())) {
      for (NameIdentifier identifier : context.parsedIdentifiers()) {
        List<MetricPoint> metrics =
            partitionPath
                .map(
                    path ->
                        metricsProvider.partitionMetrics(
                            identifier,
                            path,
                            OptimizerCommandUtils.METRICS_LIST_FROM_SECONDS,
                            OptimizerCommandUtils.METRICS_LIST_TO_SECONDS))
                .orElseGet(
                    () ->
                        metricsProvider.tableMetrics(
                            identifier,
                            OptimizerCommandUtils.METRICS_LIST_FROM_SECONDS,
                            OptimizerCommandUtils.METRICS_LIST_TO_SECONDS));
        OptimizerOutputPrinter.printMetricsResult(
            context.output(),
            partitionPath.isPresent() ? DataScope.Type.PARTITION : DataScope.Type.TABLE,
            identifier,
            partitionPath,
            metrics);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to list table metrics", e);
    }
  }
}
