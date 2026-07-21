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
import com.codahale.metrics.MetricRegistry;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.metrics.MetricNames;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHiveCatalogMetricsSource {

  @Test
  void testMetricsSourceName() {
    HiveCatalogMetricsSource source = new HiveCatalogMetricsSource("test_metalake", "test_catalog");
    Assertions.assertEquals(
        "gravitino-catalog.hive.test_metalake.test_catalog", source.getMetricsSourceName());
  }

  @Test
  void testRegisterClientPoolMetrics() {
    HiveCatalogMetricsSource source = new HiveCatalogMetricsSource("test_metalake", "test_catalog");
    MetricRegistry registry = source.getMetricRegistry();

    // Create a simple MetricsProvider with fixed values
    MetricsProvider provider =
        new MetricsProvider() {
          @Override
          public int currentSize() {
            return 8;
          }

          @Override
          public int idleCount() {
            return 3;
          }

          @Override
          public int poolSize() {
            return 10;
          }
        };

    source.registerClientPoolMetrics(provider);

    SortedMap<String, Gauge> gauges = registry.getGauges();
    Assertions.assertTrue(gauges.containsKey(MetricNames.DATASOURCE_ACTIVE_CONNECTIONS));
    Assertions.assertTrue(gauges.containsKey(MetricNames.DATASOURCE_IDLE_CONNECTIONS));
    Assertions.assertTrue(gauges.containsKey(MetricNames.DATASOURCE_MAX_CONNECTIONS));

    // active = currentSize - idleCount = 8 - 3 = 5
    Assertions.assertEquals(5, gauges.get(MetricNames.DATASOURCE_ACTIVE_CONNECTIONS).getValue());
    // idle = 3
    Assertions.assertEquals(3, gauges.get(MetricNames.DATASOURCE_IDLE_CONNECTIONS).getValue());
    // max = 10
    Assertions.assertEquals(10, gauges.get(MetricNames.DATASOURCE_MAX_CONNECTIONS).getValue());
  }

  @Test
  void testMetricsReflectDynamicProviderValues() {
    HiveCatalogMetricsSource source = new HiveCatalogMetricsSource("test_metalake", "test_catalog");
    MetricRegistry registry = source.getMetricRegistry();

    // Use AtomicInteger to simulate changing pool state
    AtomicInteger currentSize = new AtomicInteger(5);
    AtomicInteger idleCount = new AtomicInteger(2);
    AtomicInteger poolSize = new AtomicInteger(10);

    MetricsProvider provider =
        new MetricsProvider() {
          @Override
          public int currentSize() {
            return currentSize.get();
          }

          @Override
          public int idleCount() {
            return idleCount.get();
          }

          @Override
          public int poolSize() {
            return poolSize.get();
          }
        };

    source.registerClientPoolMetrics(provider);

    SortedMap<String, Gauge> gauges = registry.getGauges();

    // Initial state: active=3, idle=2, max=10
    Assertions.assertEquals(3, gauges.get(MetricNames.DATASOURCE_ACTIVE_CONNECTIONS).getValue());
    Assertions.assertEquals(2, gauges.get(MetricNames.DATASOURCE_IDLE_CONNECTIONS).getValue());
    Assertions.assertEquals(10, gauges.get(MetricNames.DATASOURCE_MAX_CONNECTIONS).getValue());

    // Simulate more users connecting: currentSize increases, idle decreases
    currentSize.set(9);
    idleCount.set(1);

    // Gauges should reflect the new values dynamically
    Assertions.assertEquals(8, gauges.get(MetricNames.DATASOURCE_ACTIVE_CONNECTIONS).getValue());
    Assertions.assertEquals(1, gauges.get(MetricNames.DATASOURCE_IDLE_CONNECTIONS).getValue());
    Assertions.assertEquals(10, gauges.get(MetricNames.DATASOURCE_MAX_CONNECTIONS).getValue());
  }

  @Test
  void testAggregatedMetricsAcrossMultiplePools() {
    // Simulate the scenario: 3 users, each with their own pool
    // User A: currentSize=3, idleCount=1, poolSize=10
    // User B: currentSize=5, idleCount=2, poolSize=10
    // User C: currentSize=2, idleCount=0, poolSize=10
    // Aggregated: currentSize=10, idleCount=3, poolSize=30

    HiveCatalogMetricsSource source = new HiveCatalogMetricsSource("test_metalake", "test_catalog");
    MetricRegistry registry = source.getMetricRegistry();

    MetricsProvider provider =
        new MetricsProvider() {
          @Override
          public int currentSize() {
            return 3 + 5 + 2; // sum across 3 pools
          }

          @Override
          public int idleCount() {
            return 1 + 2 + 0; // sum across 3 pools
          }

          @Override
          public int poolSize() {
            return 10 + 10 + 10; // sum across 3 pools (each poolSize=10)
          }
        };

    source.registerClientPoolMetrics(provider);

    SortedMap<String, Gauge> gauges = registry.getGauges();

    // active = 10 - 3 = 7
    Assertions.assertEquals(7, gauges.get(MetricNames.DATASOURCE_ACTIVE_CONNECTIONS).getValue());
    // idle = 3
    Assertions.assertEquals(3, gauges.get(MetricNames.DATASOURCE_IDLE_CONNECTIONS).getValue());
    // max = 10 + 10 + 10 = 30 (sum across 3 pools)
    Assertions.assertEquals(30, gauges.get(MetricNames.DATASOURCE_MAX_CONNECTIONS).getValue());
  }
}
