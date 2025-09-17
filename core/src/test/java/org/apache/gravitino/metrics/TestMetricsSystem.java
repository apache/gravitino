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

package org.apache.gravitino.metrics;

import org.apache.gravitino.metrics.source.MetricsSource;
import org.apache.gravitino.metrics.source.TestMetricsSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMetricsSystem {
  private MetricsSystem metricsSystem = new MetricsSystem();

  private static class MockMetricsSource extends MetricsSource {
    public MockMetricsSource(String metricsSourceName) {
      super(metricsSourceName);
    }
  }

  private static class Mock2MetricsSource extends MetricsSource {
    public Mock2MetricsSource(String metricsSourceName) {
      super(metricsSourceName);
    }
  }

  @Test
  void testRegisterMetricsWithSameMetricsSourceName() {
    MockMetricsSource metricsSource = new MockMetricsSource("a");
    Mock2MetricsSource metricsSource2 = new Mock2MetricsSource("a");
    metricsSystem.register(metricsSource);
    Assertions.assertThrowsExactly(
        IllegalStateException.class, () -> metricsSystem.register(metricsSource2));
    Assertions.assertDoesNotThrow(() -> metricsSystem.register(metricsSource));
  }

  @Test
  void testRegisterMetricsSourceInRaceCondition() {
    TestMetricsSource metricsSource = new TestMetricsSource();
    metricsSystem.register(metricsSource);
    metricsSource.incCounter("a.b");
    Assertions.assertEquals(1, getCounterValue(metricsSource.getMetricsSourceName(), "a.b"));

    metricsSystem.unregister(metricsSource);
    Assertions.assertFalse(
        metricsSystem
            .getMetricRegistry()
            .getCounters()
            .containsKey(metricsSource.getMetricsSourceName() + "a.b"));

    TestMetricsSource metricsSource2 = new TestMetricsSource();
    metricsSource2.incCounter("a.b");
    TestMetricsSource metricsSource3 = new TestMetricsSource();
    metricsSource3.incCounter("a.b");
    metricsSource3.incCounter("a.b");

    // simulate the race condition
    metricsSystem.register(metricsSource2);
    Assertions.assertEquals(1, getCounterValue(metricsSource.getMetricsSourceName(), "a.b"));
    // overwrite the old metrics source
    metricsSystem.register(metricsSource3);
    Assertions.assertEquals(2, getCounterValue(metricsSource.getMetricsSourceName(), "a.b"));
    // nothing happened
    metricsSystem.unregister(metricsSource2);
    Assertions.assertEquals(2, getCounterValue(metricsSource.getMetricsSourceName(), "a.b"));
    // unregister successfully
    metricsSystem.unregister(metricsSource3);
    Assertions.assertFalse(
        metricsSystem
            .getMetricRegistry()
            .getCounters()
            .containsKey(metricsSource.getMetricsSourceName() + "a.b"));
  }

  private long getCounterValue(String metricsSourceName, String name) {
    return metricsSystem.getMetricRegistry().counter(metricsSourceName + "." + name).getCount();
  }
}
