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

package com.datastrato.gravitino.catalog.lakehouse.iceberg.web.metrics;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.metrics.ImmutableCommitMetricsResult;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergMetricsManager {

  private MetricsReport createMetricsReport() {
    ImmutableCommitMetricsResult commitMetricsResult =
        ImmutableCommitMetricsResult.builder().build();
    MetricsReport metricsReport =
        ImmutableCommitReport.builder()
            .tableName("a")
            .snapshotId(1)
            .sequenceNumber(1)
            .operation("select")
            .commitMetrics(commitMetricsResult)
            .build();
    return metricsReport;
  }

  private MetricsReport tryGetIcebergMetrics(MemoryMetricsStore memoryMetricsStore) {
    await()
        .atMost(20, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertTrue(memoryMetricsStore.getMetricsReport() != null));
    return memoryMetricsStore.getMetricsReport();
  }

  @Test
  void testIcebergMetricsManager() {
    IcebergConfig icebergConfig = new IcebergConfig();

    IcebergMetricsManager icebergMetricsManager = new IcebergMetricsManager(icebergConfig);
    icebergMetricsManager.start();

    MetricsReport metricsReport = createMetricsReport();
    icebergMetricsManager.recordMetric(metricsReport);
    Assertions.assertDoesNotThrow(
        () -> (DummyMetricsStore) icebergMetricsManager.getIcebergMetricsStore());
    icebergMetricsManager.close();
  }

  @Test
  void testIcebergMetricsManagerWithNotExistsStoreType() {
    IcebergConfig icebergConfig =
        new IcebergConfig(
            ImmutableMap.of(IcebergMetricsManager.ICEBERG_METRICS_STORE, "not-exists"));

    Assertions.assertThrowsExactly(
        RuntimeException.class, () -> new IcebergMetricsManager(icebergConfig));
  }

  @Test
  void testIcebergMetricsManagerWithMemoryStore() throws InterruptedException {
    Map<String, String> properties =
        ImmutableMap.of(
            IcebergMetricsManager.ICEBERG_METRICS_STORE,
            "com.datastrato.gravitino.catalog.lakehouse.iceberg.web.metrics.MemoryMetricsStore",
            "a",
            "b");
    IcebergConfig icebergConfig = new IcebergConfig(properties);

    IcebergMetricsManager icebergMetricsManager = new IcebergMetricsManager(icebergConfig);
    icebergMetricsManager.start();

    MetricsReport metricsReport = createMetricsReport();
    icebergMetricsManager.recordMetric(metricsReport);
    MemoryMetricsStore memoryMetricsStore =
        (MemoryMetricsStore) icebergMetricsManager.getIcebergMetricsStore();
    Assertions.assertEquals(metricsReport, tryGetIcebergMetrics(memoryMetricsStore));
    Assertions.assertEquals(properties, memoryMetricsStore.getProperties());

    icebergMetricsManager.close();
  }
}
