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
package org.apache.gravitino.maintenance.storage.service;

import com.codahale.metrics.MetricRegistry;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.metrics.MethodMonitorAspect;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.MetricsSource;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.aspectj.lang.Aspects;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Verifies that {@code @Monitored} methods in this plugin are woven and record metrics. */
class TestMaintenanceMetaServiceMetrics extends TestJDBCBackend {

  private MethodMonitorAspect aspect;
  private Object originalMetricsSystem;
  private MetricsSource metricsSource;

  @BeforeEach
  void installMetricsSystem() throws IllegalAccessException {
    // The aspect reads the metrics system once when it is created, so replace it for this test.
    aspect = Aspects.aspectOf(MethodMonitorAspect.class);
    originalMetricsSystem = FieldUtils.readField(aspect, "metricsSystem", true);
    metricsSource = new MetricsSource(MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME) {};
    MetricsSystem metricsSystem = new MetricsSystem();
    metricsSystem.register(metricsSource);
    FieldUtils.writeField(aspect, "metricsSystem", metricsSystem, true);
  }

  @AfterEach
  void restoreMetricsSystem() throws IllegalAccessException {
    FieldUtils.writeField(aspect, "metricsSystem", originalMetricsSystem, true);
  }

  @TestTemplate
  void testMonitoredMethodsRecordMetrics() {
    TableSnapshotMetricsMetaService service = TableSnapshotMetricsMetaService.getInstance();
    long metricsId = service.insertMetrics(1L, 2L, 99L, "{\"mse\":1}");
    service.getMetrics(metricsId);

    assertMetrics("insertTableSnapshotMetrics", 1, 1, 0);
    assertMetrics("getTableSnapshotMetricsById", 1, 1, 0);
  }

  private void assertMetrics(String baseMetricName, long total, long success, long failure) {
    Assertions.assertEquals(
        total, metricsSource.getTimer(MetricRegistry.name(baseMetricName, "total")).getCount());
    Assertions.assertEquals(
        success, metricsSource.getMeter(MetricRegistry.name(baseMetricName, "success")).getCount());
    Assertions.assertEquals(
        failure, metricsSource.getMeter(MetricRegistry.name(baseMetricName, "failure")).getCount());
  }
}
