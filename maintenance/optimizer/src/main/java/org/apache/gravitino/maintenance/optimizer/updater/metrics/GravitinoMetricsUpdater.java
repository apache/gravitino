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

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.gravitino.maintenance.optimizer.api.updater.MetricsUpdater;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.JobMetricWriteRequest;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricsRepository;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.TableMetricWriteRequest;
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
  public void updateTableMetrics(List<TableMetricWriteRequest> metrics) {
    ensureInitialized();
    metricsStorage.storeTableMetrics(metrics);
  }

  @Override
  public void updateJobMetrics(List<JobMetricWriteRequest> metrics) {
    ensureInitialized();
    metricsStorage.storeJobMetrics(metrics);
  }

  @Override
  public void close() throws Exception {
    if (metricsStorage != null) {
      metricsStorage.close();
    }
  }

  private void ensureInitialized() {
    Preconditions.checkState(
        metricsStorage != null,
        "GravitinoMetricsUpdater has not been initialized. Call initialize() first.");
  }
}
