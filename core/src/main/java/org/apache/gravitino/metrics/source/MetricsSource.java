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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;

/**
 * MetricsSource provides utilities to collect specified kind metrics, all metrics must create with
 * metricRegistry. The real metric name registered to MetricSystem will be
 * "{metricsSourceName}.{name}".
 */
public abstract class MetricsSource {

  // metrics source name
  public static final String ICEBERG_REST_SERVER_METRIC_NAME = "iceberg-rest-server";
  public static final String GRAVITINO_SERVER_METRIC_NAME = "gravitino-server";
  public static final String GRAVITINO_RELATIONAL_STORE_METRIC_NAME = "gravitino-relational-store";
  public static final String JVM_METRIC_NAME = "jvm";
  private final MetricRegistry metricRegistry;
  private final String metricsSourceName;
  private final int timeSlidingWindowSeconds;

  protected MetricsSource(String name) {
    this.metricsSourceName = name;
    metricRegistry = new MetricRegistry();
    Config config = GravitinoEnv.getInstance().config();
    if (config != null) {
      this.timeSlidingWindowSeconds =
          config.get(Configs.METRICS_TIME_SLIDING_WINDOW_SECONDS).intValue();
    } else {
      // Couldn't get config when testing
      this.timeSlidingWindowSeconds = Configs.DEFAULT_METRICS_TIME_SLIDING_WINDOW_SECONDS;
    }
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
    return this.metricRegistry.histogram(
        name,
        () ->
            new Histogram(
                new SlidingTimeWindowArrayReservoir(
                    getTimeSlidingWindowSeconds(), TimeUnit.SECONDS)));
  }

  /**
   * Get or create a Timer
   *
   * @param name The name for the timer, should be unique in metrics source.
   * @return a new or pre-existing Timer
   */
  public Timer getTimer(String name) {
    return this.metricRegistry.timer(
        name,
        () ->
            new Timer(
                new SlidingTimeWindowArrayReservoir(
                    getTimeSlidingWindowSeconds(), TimeUnit.SECONDS)));
  }

  protected int getTimeSlidingWindowSeconds() {
    return timeSlidingWindowSeconds;
  }
}
