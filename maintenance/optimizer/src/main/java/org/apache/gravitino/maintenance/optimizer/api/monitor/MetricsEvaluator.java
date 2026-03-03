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
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;

/**
 * Evaluator interface for the table and related job metrics before and after optimization actions.
 */
@DeveloperApi
public interface MetricsEvaluator {

  /** Human-readable evaluator name, primarily for logging and selection. */
  String name();

  /**
   * Optional initialization hook for evaluators that need runtime configuration.
   *
   * @param optimizerEnv shared optimizer environment/configuration
   */
  default void initialize(OptimizerEnv optimizerEnv) {}

  /**
   * Evaluate metrics before/after optimization to decide success/failure.
   *
   * @param scope context of the metrics being evaluated (table/partition/job)
   * @param beforeMetrics metrics collected before the action timestamp, keyed by metric name
   * @param afterMetrics metrics collected at/after the action timestamp, keyed by metric name
   * @return true when metrics meet expectations
   */
  boolean evaluateMetrics(
      MetricScope scope,
      Map<String, List<MetricSample>> beforeMetrics,
      Map<String, List<MetricSample>> afterMetrics);
}
