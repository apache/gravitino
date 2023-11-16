/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.metrics.source;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public abstract class MetricsSource {
  protected MetricRegistry metricRegistry;
  protected String metricsSourceName;

  MetricsSource(String name) {
    this.metricsSourceName = name;
    metricRegistry = new MetricRegistry();
  }

  public String getMetricsSourceName() {
    return metricsSourceName;
  }

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  public void registerGauge(String name, Gauge gauge) {
    this.metricRegistry.registerGauge(name, gauge);
  }

  public Counter getCounter(String name) {
    return this.metricRegistry.counter(name);
  }

  public Histogram getHistogram(String name) {
    return this.metricRegistry.histogram(name);
  }

  public Timer getTimer(String name) {
    return this.metricRegistry.timer(name);
  }
}
