/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.metrics.source;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.datastrato.gravitino.metrics.MetricsSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestMetricsSource extends MetricsSource {

  private static String TEST_METRICS_SOURCE = "test";
  private static int gaugeValue = 0;
  private MetricsSystem metricsSystem;

  public TestMetricsSource() {
    super(TEST_METRICS_SOURCE);
  }

  @BeforeAll
  void init() {
    metricsSystem = new MetricsSystem();
    metricsSystem.register(this);
  }

  public void incCounter(String name) {
    getCounter(name).inc();
  }

  @Test
  void testCounter() {
    getCounter("a.counter").inc();
    long v =
        metricsSystem.getMetricRegistry().counter(TEST_METRICS_SOURCE + ".a.counter").getCount();
    Assertions.assertEquals(1, v);
    Assertions.assertEquals(1, getCounter("a.counter").getCount());
  }

  private int getGaugeValue() {
    gaugeValue++;
    return gaugeValue;
  }

  @Test
  void testGauge() {
    registerGauge("a.gauge", () -> getGaugeValue());
    Integer v =
        (Integer)
            metricsSystem.getMetricRegistry().gauge(TEST_METRICS_SOURCE + ".a.gauge").getValue();
    Assertions.assertEquals(1, v.intValue());
    v =
        (Integer)
            metricsSystem.getMetricRegistry().gauge(TEST_METRICS_SOURCE + ".a.gauge").getValue();
    Assertions.assertEquals(2, v.intValue());
  }

  @Test
  void testTimer() throws InterruptedException {
    Timer timer = getTimer("a.timer");
    for (int i = 0; i < 100; i++) {
      Timer.Context context = timer.time();
      try {
        // no ops
      } finally {
        context.stop();
      }
    }
    long v =
        metricsSystem
            .getMetricRegistry()
            .timer(TEST_METRICS_SOURCE + ".a.timer")
            .getSnapshot()
            .size();
    // it's hard to check timer values, we just check the num
    Assertions.assertEquals(100, v);
  }

  @Test
  void testHistogram() {
    Histogram histogram = getHistogram("a.histogram");
    for (int i = 0; i < 100; i++) {
      histogram.update(i);
    }

    Snapshot snapshot =
        metricsSystem
            .getMetricRegistry()
            .histogram(TEST_METRICS_SOURCE + ".a.histogram")
            .getSnapshot();
    Assertions.assertEquals(99, snapshot.getMax());
    Assertions.assertEquals(0, snapshot.getMin());
    // ExponentiallyDecayingReservoir offers a 99.9% confidence level with a 5%
    // margin of error assuming a normal distribution, and an alpha factor of 0.015,
    // which heavily biases the reservoir to the past 5 minutes of measurements.
    // Assertions.assertEquals(94.0, snapshot.get95thPercentile());
    Assertions.assertEquals(100, snapshot.size());
  }
}
