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

package org.apache.gravitino.maintenance.optimizer.updater.metrics.storage;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;

/** SPI for persisting metrics produced by the optimizer updater. */
public interface MetricsRepository extends AutoCloseable {

  /** Initialize the storage backend with configuration properties. */
  void initialize(Map<String, String> properties);

  /** Persist multiple table metrics in one call. */
  void storeTableMetrics(List<TableMetricWriteRequest> metrics);

  /** Load table-level metrics within a time window [fromSecs, toSecs) in epoch seconds. */
  Map<String, List<MetricRecord>> getTableMetrics(
      NameIdentifier nameIdentifier, long fromSecs, long toSecs);

  /** Load partition-level metrics within a time window [fromSecs, toSecs) in epoch seconds. */
  Map<String, List<MetricRecord>> getPartitionMetrics(
      NameIdentifier nameIdentifier, String partition, long fromSecs, long toSecs);

  /** Delete table metrics older than the supplied timestamp (epoch seconds), exclusive. */
  int cleanupTableMetricsBefore(long timestamp);

  /** Persist multiple job metrics in one call. */
  void storeJobMetrics(List<JobMetricWriteRequest> metrics);

  /** Load job metrics within a time window [fromSecs, toSecs) in epoch seconds. */
  Map<String, List<MetricRecord>> getJobMetrics(
      NameIdentifier nameIdentifier, long fromSecs, long toSecs);

  /** Delete job metrics older than the supplied timestamp (epoch seconds), exclusive. */
  int cleanupJobMetricsBefore(long timestamp);

  /** Close repository resources. */
  @Override
  void close();
}
