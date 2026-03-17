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

package org.apache.gravitino.maintenance.optimizer.monitor.metrics;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.DataScope;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricsRepository;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc.GenericJdbcMetricsRepository;

/**
 * {@link MetricsProvider} implementation backed by Gravitino metric storage.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li>Set {@link org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig
 *       #METRICS_PROVIDER_CONFIG} to {@value #NAME}.
 *   <li>This provider initializes an internal {@link GenericJdbcMetricsRepository} and reads
 *       metrics through {@link MetricsRepository} APIs.
 * </ul>
 *
 * <p>Behavior:
 *
 * <ul>
 *   <li>{@link #jobMetrics(NameIdentifier, long, long)} returns job metrics in the requested time
 *       range.
 *   <li>{@link #tableMetrics(NameIdentifier, long, long)} returns table metrics in the requested
 *       time range.
 *   <li>{@link #partitionMetrics(NameIdentifier, PartitionPath, long, long)} returns partition
 *       metrics using encoded partition path.
 *   <li>Storage records are returned as monitor-domain {@link MetricPoint} values.
 * </ul>
 */
public class GravitinoMetricsProvider implements MetricsProvider {

  public static final String NAME = "gravitino-metrics-provider";
  private MetricsRepository metricsRepository;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    MetricsRepository repository = new GenericJdbcMetricsRepository();
    repository.initialize(optimizerEnv.config().getAllConfig());
    this.metricsRepository = repository;
  }

  @Override
  public List<MetricPoint> jobMetrics(NameIdentifier jobIdentifier, long startTime, long endTime) {
    ensureInitialized();
    return metricsRepository.getMetrics(DataScope.forJob(jobIdentifier), startTime, endTime);
  }

  @Override
  public List<MetricPoint> tableMetrics(
      NameIdentifier tableIdentifier, long startTime, long endTime) {
    ensureInitialized();
    return metricsRepository.getMetrics(DataScope.forTable(tableIdentifier), startTime, endTime);
  }

  @Override
  public List<MetricPoint> partitionMetrics(
      NameIdentifier tableIdentifier, PartitionPath partitionPath, long startTime, long endTime) {
    ensureInitialized();
    return metricsRepository.getMetrics(
        DataScope.forPartition(tableIdentifier, partitionPath), startTime, endTime);
  }

  private void ensureInitialized() {
    Preconditions.checkState(
        metricsRepository != null, "GravitinoMetricsProvider is not initialized");
  }

  @Override
  public void close() throws Exception {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }
}
