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
package org.apache.gravitino.iceberg.service.metrics;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.metrics.MetricsReport;

/** A store API to save Apache Iceberg metrics. */
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
   * @param catalog the catalog name
   * @param namespace the namespace of the table
   * @param metricsReport the metrics to be saved
   * @throws IOException if IO error happens
   */
  void recordMetric(String catalog, Namespace namespace, MetricsReport metricsReport)
      throws IOException;

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
