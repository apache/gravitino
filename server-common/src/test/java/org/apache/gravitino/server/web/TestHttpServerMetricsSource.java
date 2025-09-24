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

package org.apache.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jersey2.InstrumentedResourceMethodApplicationListener;
import org.apache.gravitino.metrics.MetricNames;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestHttpServerMetricsSource {

  private JettyServer mockJettyServer;
  private ThreadPool mockThreadPool;
  private ResourceConfig mockResourceConfig;

  private HttpServerMetricsSource metricsSource;

  @BeforeEach
  void setUp() {
    mockJettyServer = mock(JettyServer.class);
    mockThreadPool = mock(ThreadPool.class);
    mockResourceConfig = mock(ResourceConfig.class);
  }

  @Test
  void testConstructorWithQueuedThreadPool() {
    // Arrange - Use a real QueuedThreadPool instance
    QueuedThreadPool realQueuedThreadPool = new QueuedThreadPool(20, 1, 60000);
    when(mockJettyServer.getThreadPool()).thenReturn(realQueuedThreadPool);

    // Act
    metricsSource = new HttpServerMetricsSource("test-server", mockResourceConfig, mockJettyServer);

    // Assert
    assertNotNull(metricsSource);
    assertNotNull(metricsSource.getMetricRegistry());

    // Verify ResourceConfig registration
    verify(mockResourceConfig).register(any(InstrumentedResourceMethodApplicationListener.class));

    // Verify basic thread pool metrics are registered
    MetricRegistry registry = metricsSource.getMetricRegistry();
    assertTrue(registry.getGauges().containsKey(MetricNames.SERVER_IDLE_THREAD_NUM));
    assertTrue(registry.getGauges().containsKey(MetricNames.SERVER_TOTAL_THREAD_NUM));

    // Verify QueuedThreadPool specific metrics are registered
    assertTrue(registry.getGauges().containsKey(MetricNames.SERVER_BUSY_THREAD_NUM));
    assertTrue(registry.getGauges().containsKey(MetricNames.SERVER_QUEUED_REQUEST_NUM));
    assertTrue(registry.getGauges().containsKey(MetricNames.SERVER_MIN_THREAD_NUM));
    assertTrue(registry.getGauges().containsKey(MetricNames.SERVER_MAX_THREAD_NUM));
  }

  @Test
  void testConstructorWithNonQueuedThreadPool() {
    // Arrange - Use a mock ThreadPool that is not QueuedThreadPool
    when(mockJettyServer.getThreadPool()).thenReturn(mockThreadPool);
    when(mockThreadPool.getIdleThreads()).thenReturn(3);
    when(mockThreadPool.getThreads()).thenReturn(8);

    // Act
    metricsSource = new HttpServerMetricsSource("test-server", mockResourceConfig, mockJettyServer);

    // Assert
    assertNotNull(metricsSource);
    assertNotNull(metricsSource.getMetricRegistry());

    // Verify ResourceConfig registration
    verify(mockResourceConfig).register(any(InstrumentedResourceMethodApplicationListener.class));

    // Verify only basic thread pool metrics are registered
    MetricRegistry registry = metricsSource.getMetricRegistry();
    assertTrue(registry.getGauges().containsKey(MetricNames.SERVER_IDLE_THREAD_NUM));
    assertTrue(registry.getGauges().containsKey(MetricNames.SERVER_TOTAL_THREAD_NUM));

    // Verify QueuedThreadPool specific metrics are NOT registered
    assertTrue(
        registry.getGauges().isEmpty()
            || !registry.getGauges().containsKey(MetricNames.SERVER_BUSY_THREAD_NUM));
    assertTrue(
        registry.getGauges().isEmpty()
            || !registry.getGauges().containsKey(MetricNames.SERVER_QUEUED_REQUEST_NUM));
    assertTrue(
        registry.getGauges().isEmpty()
            || !registry.getGauges().containsKey(MetricNames.SERVER_MIN_THREAD_NUM));
    assertTrue(
        registry.getGauges().isEmpty()
            || !registry.getGauges().containsKey(MetricNames.SERVER_MAX_THREAD_NUM));
  }

  @Test
  void testGaugeValuesWithQueuedThreadPool() {
    // Arrange - Use a real QueuedThreadPool instance
    QueuedThreadPool realQueuedThreadPool = new QueuedThreadPool(20, 1, 60000);
    when(mockJettyServer.getThreadPool()).thenReturn(realQueuedThreadPool);

    // Act
    metricsSource = new HttpServerMetricsSource("test-server", mockResourceConfig, mockJettyServer);

    // Assert - Test gauge values
    MetricRegistry registry = metricsSource.getMetricRegistry();

    @SuppressWarnings("unchecked")
    Gauge<Integer> idleThreadsGauge =
        (Gauge<Integer>) registry.getGauges().get(MetricNames.SERVER_IDLE_THREAD_NUM);
    assertTrue(idleThreadsGauge.getValue() >= 0, "Idle threads should be non-negative");

    @SuppressWarnings("unchecked")
    Gauge<Integer> totalThreadsGauge =
        (Gauge<Integer>) registry.getGauges().get(MetricNames.SERVER_TOTAL_THREAD_NUM);
    assertTrue(totalThreadsGauge.getValue() >= 0, "Total threads should be non-negative");

    @SuppressWarnings("unchecked")
    Gauge<Integer> busyThreadsGauge =
        (Gauge<Integer>) registry.getGauges().get(MetricNames.SERVER_BUSY_THREAD_NUM);
    assertTrue(busyThreadsGauge.getValue() >= 0, "Busy threads should be non-negative");

    @SuppressWarnings("unchecked")
    Gauge<Integer> queuedRequestsGauge =
        (Gauge<Integer>) registry.getGauges().get(MetricNames.SERVER_QUEUED_REQUEST_NUM);
    assertTrue(queuedRequestsGauge.getValue() >= 0, "Queued requests should be non-negative");

    @SuppressWarnings("unchecked")
    Gauge<Integer> minThreadsGauge =
        (Gauge<Integer>) registry.getGauges().get(MetricNames.SERVER_MIN_THREAD_NUM);
    assertEquals(1, minThreadsGauge.getValue().intValue());

    @SuppressWarnings("unchecked")
    Gauge<Integer> maxThreadsGauge =
        (Gauge<Integer>) registry.getGauges().get(MetricNames.SERVER_MAX_THREAD_NUM);
    assertEquals(20, maxThreadsGauge.getValue().intValue());
  }

  @Test
  void testGaugeValuesWithNonQueuedThreadPool() {
    // Arrange
    when(mockJettyServer.getThreadPool()).thenReturn(mockThreadPool);
    when(mockThreadPool.getIdleThreads()).thenReturn(3);
    when(mockThreadPool.getThreads()).thenReturn(8);

    // Act
    metricsSource = new HttpServerMetricsSource("test-server", mockResourceConfig, mockJettyServer);

    // Assert - Test gauge values
    MetricRegistry registry = metricsSource.getMetricRegistry();

    @SuppressWarnings("unchecked")
    Gauge<Integer> idleThreadsGauge =
        (Gauge<Integer>) registry.getGauges().get(MetricNames.SERVER_IDLE_THREAD_NUM);
    assertEquals(3, idleThreadsGauge.getValue().intValue());

    @SuppressWarnings("unchecked")
    Gauge<Integer> totalThreadsGauge =
        (Gauge<Integer>) registry.getGauges().get(MetricNames.SERVER_TOTAL_THREAD_NUM);
    assertEquals(8, totalThreadsGauge.getValue().intValue());
  }

  @Test
  void testResourceConfigRegistration() {
    // Arrange
    QueuedThreadPool realQueuedThreadPool = new QueuedThreadPool(10, 5, 60000);
    when(mockJettyServer.getThreadPool()).thenReturn(realQueuedThreadPool);

    // Act
    metricsSource = new HttpServerMetricsSource("test-server", mockResourceConfig, mockJettyServer);

    // Assert
    ArgumentCaptor<InstrumentedResourceMethodApplicationListener> captor =
        ArgumentCaptor.forClass(InstrumentedResourceMethodApplicationListener.class);
    verify(mockResourceConfig).register(captor.capture());

    InstrumentedResourceMethodApplicationListener listener = captor.getValue();
    assertNotNull(listener);
  }

  @Test
  void testMetricsSourceName() {
    // Arrange
    when(mockJettyServer.getThreadPool()).thenReturn(mockThreadPool);
    when(mockThreadPool.getIdleThreads()).thenReturn(0);
    when(mockThreadPool.getThreads()).thenReturn(0);

    // Act
    metricsSource = new HttpServerMetricsSource("test-server", mockResourceConfig, mockJettyServer);

    // Assert
    assertEquals("test-server", metricsSource.getMetricsSourceName());
  }

  @Test
  void testGaugeLambdasAreCalled() {
    // Arrange
    when(mockJettyServer.getThreadPool()).thenReturn(mockThreadPool);
    when(mockThreadPool.getIdleThreads()).thenReturn(5);
    when(mockThreadPool.getThreads()).thenReturn(10);

    // Act
    metricsSource = new HttpServerMetricsSource("test-server", mockResourceConfig, mockJettyServer);

    // Assert - Verify that the gauge lambdas call the thread pool methods
    MetricRegistry registry = metricsSource.getMetricRegistry();

    // Call getValue() on each gauge to trigger the lambda
    registry.getGauges().get(MetricNames.SERVER_IDLE_THREAD_NUM).getValue();
    registry.getGauges().get(MetricNames.SERVER_TOTAL_THREAD_NUM).getValue();

    // Verify that the thread pool methods were called
    verify(mockThreadPool).getIdleThreads();
    verify(mockThreadPool).getThreads();
  }

  @Test
  void testQueuedThreadPoolCast() {
    // Arrange - Create a real QueuedThreadPool instance
    QueuedThreadPool realQueuedThreadPool = new QueuedThreadPool(10, 5, 60000);
    when(mockJettyServer.getThreadPool()).thenReturn(realQueuedThreadPool);

    // Act
    metricsSource = new HttpServerMetricsSource("test-server", mockResourceConfig, mockJettyServer);

    // Assert
    MetricRegistry registry = metricsSource.getMetricRegistry();

    // Verify all metrics are registered including QueuedThreadPool specific ones
    assertTrue(registry.getGauges().containsKey(MetricNames.SERVER_IDLE_THREAD_NUM));
    assertTrue(registry.getGauges().containsKey(MetricNames.SERVER_TOTAL_THREAD_NUM));
    assertTrue(registry.getGauges().containsKey(MetricNames.SERVER_BUSY_THREAD_NUM));
    assertTrue(registry.getGauges().containsKey(MetricNames.SERVER_QUEUED_REQUEST_NUM));
    assertTrue(registry.getGauges().containsKey(MetricNames.SERVER_MIN_THREAD_NUM));
    assertTrue(registry.getGauges().containsKey(MetricNames.SERVER_MAX_THREAD_NUM));

    // Verify gauge values are reasonable (not null and non-negative)
    registry
        .getGauges()
        .values()
        .forEach(
            gauge -> {
              Object value = gauge.getValue();
              assertNotNull(value);
              if (value instanceof Integer) {
                assertTrue((Integer) value >= 0, "Gauge value should be non-negative: " + value);
              }
            });
  }
}
