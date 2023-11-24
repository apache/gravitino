/*
 *  Copyright 2023 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.web.metrics;

import java.time.Instant;
import java.util.Map;
import org.apache.iceberg.metrics.MetricsReport;

public class BlackHoleMetricsStore implements IcebergMetricsStore {
  public static final String ICEBERG_METRICS_STORE_BLACK_HOLE_NAME = "blackHole";

  @Override
  public void init(Map<String, String> properties) {}

  @Override
  public void save(MetricsReport metricsReport) {}

  @Override
  public void close() {}

  @Override
  public synchronized void clean(Instant expireTime) {}
}
