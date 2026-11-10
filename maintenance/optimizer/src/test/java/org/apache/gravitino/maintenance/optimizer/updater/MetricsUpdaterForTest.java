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

package org.apache.gravitino.maintenance.optimizer.updater;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.updater.MetricsUpdater;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;

public class MetricsUpdaterForTest implements MetricsUpdater {

  public static final String NAME = "test-metrics-updater";
  private static final List<MetricsUpdaterForTest> INSTANCES =
      Collections.synchronizedList(new ArrayList<>());
  private final AtomicInteger tableAndPartitionUpdates = new AtomicInteger();
  private final AtomicInteger jobUpdates = new AtomicInteger();
  private final AtomicInteger closeCalls = new AtomicInteger();
  private volatile List<MetricPoint> lastTableAndPartitionMetrics = List.of();
  private volatile List<MetricPoint> lastJobMetrics = List.of();

  public MetricsUpdaterForTest() {
    INSTANCES.add(this);
  }

  public static List<MetricsUpdaterForTest> instances() {
    return new ArrayList<>(INSTANCES);
  }

  public static void reset() {
    INSTANCES.clear();
  }

  public int tableAndPartitionUpdates() {
    return tableAndPartitionUpdates.get();
  }

  public List<MetricPoint> lastTableAndPartitionMetrics() {
    return lastTableAndPartitionMetrics;
  }

  public int jobUpdates() {
    return jobUpdates.get();
  }

  public List<MetricPoint> lastJobMetrics() {
    return lastJobMetrics;
  }

  public int closeCalls() {
    return closeCalls.get();
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {}

  @Override
  public void updateTableAndPartitionMetrics(List<MetricPoint> metrics) {
    tableAndPartitionUpdates.incrementAndGet();
    lastTableAndPartitionMetrics = metrics == null ? List.of() : List.copyOf(metrics);
  }

  @Override
  public void updateJobMetrics(List<MetricPoint> metrics) {
    jobUpdates.incrementAndGet();
    lastJobMetrics = metrics == null ? List.of() : List.copyOf(metrics);
  }

  @Override
  public void close() {
    closeCalls.incrementAndGet();
  }
}
