/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.metrics;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.metrics.source.TestMetricsSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.mockito.Mockito;

@TestInstance(Lifecycle.PER_CLASS)
public class TestMetricsSystem {
  private MetricsSystem metricsSystem;

  @BeforeAll
  void init() {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.METRICS_TIME_SLIDING_WINDOW_SECONDS)).thenReturn(60);
    metricsSystem = new MetricsSystem(config);
  }

  @Test
  void testRegisterMetricsSource() {
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
