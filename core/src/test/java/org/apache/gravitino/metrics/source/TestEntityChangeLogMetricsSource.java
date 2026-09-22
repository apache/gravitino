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
package org.apache.gravitino.metrics.source;

import com.codahale.metrics.Gauge;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the nonblocking state gauges and counters of the entity change log source. */
public class TestEntityChangeLogMetricsSource {
  @Test
  void testPollAndFailureMetrics() {
    EntityChangeLogMetricsSource metrics = new EntityChangeLogMetricsSource();
    Assertions.assertEquals(-1L, gauge(metrics, "seconds-since-last-successful-poll"));

    metrics.setCursorId(5);
    metrics.setDbTailId(8);
    metrics.pollSucceeded(3);
    metrics.recordsDelivered("org.apache.gravitino.CacheListener", 6);
    metrics.recordsApplied(6);
    metrics.pollFailed();
    metrics.listenerFailed("org.apache.gravitino.CacheListener");
    metrics.invalidationFailed();
    metrics.fallbackCleared();

    Assertions.assertEquals(5L, gauge(metrics, "cursor-id"));
    Assertions.assertEquals(8L, gauge(metrics, "db-tail-id"));
    Assertions.assertEquals(3L, gauge(metrics, "record-lag"));
    Assertions.assertTrue(gauge(metrics, "seconds-since-last-successful-poll") >= 0);
    Assertions.assertEquals(
        3, metrics.getMetricRegistry().counter("records-fetched-total").getCount());
    Assertions.assertEquals(
        6, metrics.getMetricRegistry().counter("records-delivered-total").getCount());
    Assertions.assertEquals(
        6,
        metrics
            .getMetricRegistry()
            .counter("records-delivered.org_apache_gravitino_CacheListener-total")
            .getCount());
    Assertions.assertEquals(
        6, metrics.getMetricRegistry().counter("records-applied-total").getCount());
    Assertions.assertEquals(
        1, metrics.getMetricRegistry().counter("poll-failures-total").getCount());
    Assertions.assertEquals(
        1, metrics.getMetricRegistry().counter("listener-failures-total").getCount());
    Assertions.assertEquals(
        1,
        metrics
            .getMetricRegistry()
            .counter("listener-failures.org_apache_gravitino_CacheListener-total")
            .getCount());
    Assertions.assertEquals(
        1, metrics.getMetricRegistry().counter("invalidation-failures-total").getCount());
    Assertions.assertEquals(
        1, metrics.getMetricRegistry().counter("fallback-clears-total").getCount());
    Assertions.assertEquals(
        1, metrics.getMetricRegistry().histogram("batch-size-records").getCount());
  }

  private static long gauge(EntityChangeLogMetricsSource metrics, String name) {
    Gauge<?> gauge = metrics.getMetricRegistry().getGauges().get(name);
    return ((Number) gauge.getValue()).longValue();
  }
}
