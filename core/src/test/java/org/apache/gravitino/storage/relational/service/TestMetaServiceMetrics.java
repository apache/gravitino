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
package org.apache.gravitino.storage.relational.service;

import com.codahale.metrics.MetricRegistry;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.metrics.MethodMonitorAspect;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.MetricsSource;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.aspectj.lang.Aspects;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Verifies that {@code @Monitored} methods in core are woven and record metrics. */
class TestMetaServiceMetrics extends TestJDBCBackend {

  private MethodMonitorAspect aspect;
  private MetricsSystem originalMetricsSystem;
  private MetricsSource metricsSource;

  @BeforeEach
  void installMetricsSystem() {
    // The aspect reads the metrics system once when it is created, so replace it for this test.
    aspect = Aspects.aspectOf(MethodMonitorAspect.class);
    MetricsSystem metricsSystem = new MetricsSystem();
    originalMetricsSystem = aspect.replaceMetricsSystem(metricsSystem);
    metricsSource = new MetricsSource(MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME) {};
    metricsSystem.register(metricsSource);
  }

  @AfterEach
  void restoreMetricsSystem() {
    aspect.replaceMetricsSystem(originalMetricsSystem);
  }

  @TestTemplate
  void testMonitoredMethodsRecordMetrics() {
    MetalakeMetaService service = MetalakeMetaService.getInstance();
    service.listMetalakes();
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> service.getMetalakeIdByName("missing_metalake"));

    assertMetrics("listMetalakes", 1, 1, 0);
    assertMetrics("getMetalakeIdByName", 1, 0, 1);
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
