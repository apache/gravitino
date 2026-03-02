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

package org.apache.gravitino.maintenance.optimizer.api.monitor;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.Provider;

/** Represents a provider that provides table and job related metrics. */
@DeveloperApi
public interface MetricsProvider extends Provider {
  /**
   * Retrieve metrics for a specific job within a time range.
   *
   * @param jobIdentifier catalog/schema/job identifier
   * @param startTime start timestamp (seconds)
   * @param endTime end timestamp (seconds)
   * @return map keyed by metric name, each containing a metric sample series
   */
  Map<String, List<MetricSample>> jobMetrics(
      NameIdentifier jobIdentifier, long startTime, long endTime);

  /**
   * Retrieve metrics for a table.
   *
   * @param tableIdentifier catalog/schema/table identifier
   * @param startTime start timestamp (seconds)
   * @param endTime end timestamp (seconds)
   * @return map keyed by metric name, each containing a metric sample series
   */
  Map<String, List<MetricSample>> tableMetrics(
      NameIdentifier tableIdentifier, long startTime, long endTime);

  /**
   * Retrieve metrics for a specific partition of a table.
   *
   * @param tableIdentifier catalog/schema/table identifier
   * @param partitionPath partition path
   * @param startTime start timestamp (seconds)
   * @param endTime end timestamp (seconds)
   * @return map keyed by metric name, each containing a metric sample series
   */
  Map<String, List<MetricSample>> partitionMetrics(
      NameIdentifier tableIdentifier, PartitionPath partitionPath, long startTime, long endTime);
}
