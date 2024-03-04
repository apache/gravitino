/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.metrics.source;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * MetricsSource provides utilities to collect specified kind metrics, all metrics must create with
 * metricRegistry. The real metric name registered to MetricSystem will be
 * "{metricsSourceName}.{name}".
 */
public abstract class MetricsSource {

  // metrics source name
  public static final String ICEBERG_REST_SERVER_METRIC_NAME = "iceberg-rest-server";
  public static final String GRAVITINO_SERVER_METRIC_NAME = "gravitino-server";
  public static final String JVM_METRIC_NAME = "jvm";
  private final MetricRegistry metricRegistry;
  private final String metricsSourceName;

  protected MetricsSource(String name) {
    this.metricsSourceName = name;
    metricRegistry = new MetricRegistry();
  }

  /**
   * Get metrics source name, the name should be unique in MetricsSystem
   *
   * @return metrics source name
   */
  public String getMetricsSourceName() {
    return metricsSourceName;
  }

  /**
   * Get metric registry, it's mainly used to register metrics to MetricsSystem
   *
   * @return metric registry
   */
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  /**
   * Register a Gauge
   *
   * @param name The name for the gauge, should be unique in metrics source.
   * @param gauge The function to get gauge value.
   */
  public void registerGauge(String name, Gauge gauge) {
    this.metricRegistry.registerGauge(name, gauge);
  }

  /**
   * Get or create a Counter
   *
   * @param name The name for the counter, should be unique in metrics source.
   * @return a new or pre-existing Counter
   */
  public Counter getCounter(String name) {
    return this.metricRegistry.counter(name);
  }

  /**
   * Get or create a Histogram
   *
   * @param name The name for the histogram, should be unique in metrics source.
   * @return a new or pre-existing Histogram
   */
  public Histogram getHistogram(String name) {
    return this.metricRegistry.histogram(name);
  }

  /**
   * Get or create a Timer
   *
   * @param name The name for the timer, should be unique in metrics source.
   * @return a new or pre-existing Timer
   */
  public Timer getTimer(String name) {
    return this.metricRegistry.timer(name);
  }
}
