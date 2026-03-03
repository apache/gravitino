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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.maintenance.optimizer.api.common.DataScope;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricValueSample;

/** Immutable evaluation result passed to monitor callbacks. */
@DeveloperApi
public class EvaluationResult {

  private final DataScope scope;
  private final boolean evaluation;
  private final Map<String, List<MetricValueSample>> beforeMetrics;
  private final Map<String, List<MetricValueSample>> afterMetrics;
  private final long actionTimeSeconds;
  private final long rangeSeconds;
  private final String evaluatorName;

  /**
   * Create an immutable evaluation result snapshot.
   *
   * @param scope scope of the evaluated metrics
   * @param evaluation evaluation outcome from the evaluator
   * @param beforeMetrics metric samples collected before the action timestamp
   * @param afterMetrics metric samples collected at/after the action timestamp
   * @param actionTimeSeconds action timestamp in epoch seconds
   * @param rangeSeconds evaluation half-window in seconds
   * @param evaluatorName evaluator implementation name
   */
  public EvaluationResult(
      DataScope scope,
      boolean evaluation,
      Map<String, List<MetricValueSample>> beforeMetrics,
      Map<String, List<MetricValueSample>> afterMetrics,
      long actionTimeSeconds,
      long rangeSeconds,
      String evaluatorName) {
    Preconditions.checkArgument(scope != null, "scope must not be null");
    Preconditions.checkArgument(beforeMetrics != null, "beforeMetrics must not be null");
    Preconditions.checkArgument(afterMetrics != null, "afterMetrics must not be null");
    Preconditions.checkArgument(evaluatorName != null, "evaluatorName must not be null");
    this.scope = scope;
    this.evaluation = evaluation;
    this.beforeMetrics = immutableCopy(beforeMetrics);
    this.afterMetrics = immutableCopy(afterMetrics);
    this.actionTimeSeconds = actionTimeSeconds;
    this.rangeSeconds = rangeSeconds;
    this.evaluatorName = evaluatorName;
  }

  /**
   * @return evaluated scope (table/partition/job).
   */
  public DataScope scope() {
    return scope;
  }

  /**
   * @return true if evaluator considers this scope successful.
   */
  public boolean evaluation() {
    return evaluation;
  }

  /**
   * @return immutable metric samples before the action timestamp.
   */
  public Map<String, List<MetricValueSample>> beforeMetrics() {
    return beforeMetrics;
  }

  /**
   * @return immutable metric samples at/after the action timestamp.
   */
  public Map<String, List<MetricValueSample>> afterMetrics() {
    return afterMetrics;
  }

  /**
   * @return action timestamp in epoch seconds.
   */
  public long actionTimeSeconds() {
    return actionTimeSeconds;
  }

  /**
   * @return evaluation half-window in seconds.
   */
  public long rangeSeconds() {
    return rangeSeconds;
  }

  /**
   * @return evaluator implementation name.
   */
  public String evaluatorName() {
    return evaluatorName;
  }

  private static Map<String, List<MetricValueSample>> immutableCopy(
      Map<String, List<MetricValueSample>> metrics) {
    if (metrics.isEmpty()) {
      return Map.of();
    }

    Map<String, List<MetricValueSample>> copied = new LinkedHashMap<>();
    for (Map.Entry<String, List<MetricValueSample>> entry : metrics.entrySet()) {
      Preconditions.checkArgument(entry.getKey() != null, "metric name must not be null");
      List<MetricValueSample> values = entry.getValue();
      Preconditions.checkArgument(values != null, "metric values must not be null");
      copied.put(entry.getKey(), Collections.unmodifiableList(List.copyOf(values)));
    }
    return Collections.unmodifiableMap(copied);
  }
}
