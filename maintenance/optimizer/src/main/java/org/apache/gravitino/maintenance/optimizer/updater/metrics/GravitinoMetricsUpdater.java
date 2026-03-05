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

package org.apache.gravitino.maintenance.optimizer.updater.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.gravitino.maintenance.optimizer.api.common.DataScope;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.updater.MetricsUpdater;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricsRepository;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc.GenericJdbcMetricsRepository;

/** Metrics updater that persists table/job metrics into the configured metrics repository. */
public class GravitinoMetricsUpdater implements MetricsUpdater {

  public static final String NAME = "gravitino-metrics-updater";

  private MetricsRepository metricsStorage;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    this.metricsStorage = new GenericJdbcMetricsRepository();
    this.metricsStorage.initialize(optimizerEnv.config().getAllConfig());
  }

  @Override
  public void updateTableAndPartitionMetrics(List<MetricPoint> metrics) {
    ensureInitialized();
    validateScopes(
        metrics, List.of(DataScope.Type.TABLE, DataScope.Type.PARTITION), "table/partition");
    metricsStorage.storeTableAndPartitionMetrics(metrics);
  }

  @Override
  public void updateJobMetrics(List<MetricPoint> metrics) {
    ensureInitialized();
    validateScopes(metrics, List.of(DataScope.Type.JOB), "job");
    metricsStorage.storeJobMetrics(metrics);
  }

  @Override
  public void close() throws Exception {
    if (metricsStorage != null) {
      metricsStorage.close();
    }
  }

  @VisibleForTesting
  void setMetricsRepositoryForTest(MetricsRepository metricsRepository) {
    this.metricsStorage = metricsRepository;
  }

  @VisibleForTesting
  MetricsRepository metricsRepositoryForTest() {
    return metricsStorage;
  }

  private void ensureInitialized() {
    Preconditions.checkState(
        metricsStorage != null,
        "GravitinoMetricsUpdater has not been initialized. Call initialize() first.");
  }

  private void validateScopes(
      List<MetricPoint> metrics, List<DataScope.Type> allowedScopes, String updateType) {
    Preconditions.checkArgument(metrics != null, "metrics must not be null");
    for (MetricPoint metric : metrics) {
      Preconditions.checkArgument(metric != null, "metric must not be null");
      Preconditions.checkArgument(
          allowedScopes.contains(metric.scope()),
          "Unsupported scope %s for %s metrics update",
          metric.scope(),
          updateType);
    }
  }
}
