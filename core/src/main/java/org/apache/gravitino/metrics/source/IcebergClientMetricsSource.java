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

/**
 * Metrics source for Iceberg REST server client operations.
 *
 * <p>Exposes metrics from Iceberg client operations (commits and scans) through Gravitino's
 * MetricsSystem. These metrics are collected from {@link org.apache.iceberg.metrics.MetricsReport}
 * instances sent by clients to the REST metrics endpoint.
 *
 * <p>Example metrics exposed:
 *
 * <ul>
 *   <li>{@code iceberg.total-duration} - Commit/scan operation duration
 *   <li>{@code iceberg.added-data-files} - Number of data files added in commits
 *   <li>{@code iceberg.result-data-files} - Number of data files in scan results
 *   <li>{@code iceberg.total-planning-duration} - Scan planning time
 * </ul>
 *
 * <p>Metrics are accessible via:
 *
 * <ul>
 *   <li>{@code /metrics} endpoint (JSON format)
 *   <li>{@code /prometheus/metrics} endpoint (Prometheus format)
 * </ul>
 */
public class IcebergClientMetricsSource extends MetricsSource {
  /**
   * Creates a new IcebergClientMetricsSource.
   *
   * <p>Registers this metrics source with Gravitino's MetricsSystem using the Iceberg client metric
   * namespace to separate it from HTTP server metrics.
   */
  public IcebergClientMetricsSource() {
    super(MetricsSource.ICEBERG_CLIENT_METRIC_NAME);
  }
}
