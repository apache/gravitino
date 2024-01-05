/*
 *  Copyright 2023 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.web.metrics;

import java.time.Instant;
import java.util.Map;
import org.apache.iceberg.metrics.MetricsReport;

/** Store Iceberg metrics in memory, used for test */
public class MemoryMetricsStore implements IcebergMetricsStore {
  private MetricsReport metricsReport;
  private Instant recordTime = Instant.now();
  private Map<String, String> properties;

  @Override
  public void init(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public void recordMetric(MetricsReport metricsReport) {
    this.metricsReport = metricsReport;
    this.recordTime = Instant.now();
  }

  @Override
  public void close() {}

  @Override
  public void clean(Instant expireTime) {
    if (expireTime.isBefore(recordTime)) {
      metricsReport = null;
    }
  }

  MetricsReport getMetricsReport() {
    return metricsReport;
  }

  Map<String, String> getProperties() {
    return properties;
  }
}
