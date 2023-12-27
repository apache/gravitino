/*
 *  Copyright 2023 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.web.metrics;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.iceberg.metrics.MetricsReport;

/** A store API to save Iceberg metrics. */
public interface IcebergMetricsStore {

  /**
   * Init metrics store.
   *
   * @param properties, contains all configurations start with "gravitino.auxService.iceberg-rest.".
   * @throws IOException if IO error happens
   */
  void init(Map<String, String> properties) throws IOException;

  /**
   * Record metrics report.
   *
   * @param metricsReport the metrics to be saved
   * @throws IOException if IO error happens
   */
  void recordMetric(MetricsReport metricsReport) throws IOException;

  /**
   * Clean the expired Iceberg metrics
   *
   * @param expireTime the metrics before this time should be cleaned
   * @throws IOException if IO error happens
   */
  void clean(Instant expireTime) throws IOException;

  /**
   * Close the Iceberg metrics store
   *
   * @throws IOException if IO error happens
   */
  void close() throws IOException;
}
