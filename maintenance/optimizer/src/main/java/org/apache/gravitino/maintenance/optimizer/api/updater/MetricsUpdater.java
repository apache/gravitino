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

package org.apache.gravitino.maintenance.optimizer.api.updater;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.Provider;

/** Represents an updater that can update metrics for a table or job. */
@DeveloperApi
public interface MetricsUpdater extends Provider {
  /**
   * Persist table metrics.
   *
   * @param nameIdentifier catalog/schema/table identifier
   * @param metrics time-series samples to write
   */
  void updateTableMetrics(NameIdentifier nameIdentifier, List<MetricSample> metrics);

  /**
   * Persist job metrics.
   *
   * @param nameIdentifier job identifier
   * @param metrics time-series samples to write
   */
  void updateJobMetrics(NameIdentifier nameIdentifier, List<MetricSample> metrics);
}
