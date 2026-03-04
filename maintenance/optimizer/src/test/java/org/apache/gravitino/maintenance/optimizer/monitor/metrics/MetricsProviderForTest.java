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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.DataScope;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.monitor.job.TableJobRelationProviderForTest;
import org.apache.gravitino.stats.StatisticValues;

public class MetricsProviderForTest implements MetricsProvider {

  public static final String NAME = "metrics-provider-for-test";
  public static final AtomicInteger PARTITION_METRICS_CALLS = new AtomicInteger();
  public static volatile PartitionPath LAST_PARTITION_PATH = null;
  private static volatile boolean INCLUDE_INVALID_SCOPE_METRIC = false;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {}

  @Override
  public List<MetricPoint> jobMetrics(NameIdentifier jobIdentifier, long startTime, long endTime) {
    if (TableJobRelationProviderForTest.JOB1.equals(jobIdentifier)) {
      return List.of(
          metric(DataScope.Type.JOB, jobIdentifier, null, 99, "duration", 10L),
          metric(DataScope.Type.JOB, jobIdentifier, null, 102, "duration", 20L));
    }
    if (TableJobRelationProviderForTest.JOB2.equals(jobIdentifier)) {
      return List.of(
          metric(DataScope.Type.JOB, jobIdentifier, null, 98, "duration", 30L),
          metric(DataScope.Type.JOB, jobIdentifier, null, 104, "duration", 40L));
    }
    return List.of();
  }

  @Override
  public List<MetricPoint> tableMetrics(
      NameIdentifier tableIdentifier, long startTime, long endTime) {
    List<MetricPoint> points = new ArrayList<>();
    points.add(metric(DataScope.Type.TABLE, tableIdentifier, null, 95, "row_count", 100L));
    points.add(metric(DataScope.Type.TABLE, tableIdentifier, null, 100, "row_count", 200L));
    if (INCLUDE_INVALID_SCOPE_METRIC) {
      points.add(metric(DataScope.Type.JOB, tableIdentifier, null, 96, "row_count", 999L));
    }
    return List.copyOf(points);
  }

  @Override
  public List<MetricPoint> partitionMetrics(
      NameIdentifier tableIdentifier, PartitionPath partitionPath, long startTime, long endTime) {
    PARTITION_METRICS_CALLS.incrementAndGet();
    LAST_PARTITION_PATH = partitionPath;
    return List.of(
        metric(DataScope.Type.PARTITION, tableIdentifier, partitionPath, 97, "row_count", 110L),
        metric(DataScope.Type.PARTITION, tableIdentifier, partitionPath, 101, "row_count", 210L));
  }

  @Override
  public void close() throws Exception {}

  public static void reset() {
    PARTITION_METRICS_CALLS.set(0);
    LAST_PARTITION_PATH = null;
    INCLUDE_INVALID_SCOPE_METRIC = false;
  }

  public static void includeInvalidScopeMetric(boolean includeInvalidScopeMetric) {
    INCLUDE_INVALID_SCOPE_METRIC = includeInvalidScopeMetric;
  }

  private static MetricPoint metric(
      DataScope.Type scope,
      NameIdentifier identifier,
      PartitionPath partitionPath,
      long timestamp,
      String metricName,
      long value) {
    switch (scope) {
      case TABLE:
        return MetricPoint.forTable(
            identifier, metricName, StatisticValues.longValue(value), timestamp);
      case PARTITION:
        return MetricPoint.forPartition(
            identifier, partitionPath, metricName, StatisticValues.longValue(value), timestamp);
      case JOB:
        return MetricPoint.forJob(
            identifier, metricName, StatisticValues.longValue(value), timestamp);
      default:
        throw new IllegalArgumentException("Unsupported scope: " + scope);
    }
  }
}
