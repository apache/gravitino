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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;

/** Immutable evaluation result passed to monitor callbacks. */
@DeveloperApi
public class EvaluationResult {

  private final MetricScope scope;
  private final boolean evaluation;
  private final Map<String, List<MetricSample>> beforeMetrics;
  private final Map<String, List<MetricSample>> afterMetrics;
  private final long actionTimeSeconds;
  private final long rangeSeconds;
  private final String evaluatorName;

  /**
   * Create an immutable evaluation result snapshot.
   *
   * @param scope scope of the evaluated metrics
   * @param evaluation evaluation outcome from the evaluator
   * @param beforeMetrics metrics collected before the action timestamp
   * @param afterMetrics metrics collected at/after the action timestamp
   * @param actionTimeSeconds action timestamp in epoch seconds
   * @param rangeSeconds evaluation half-window in seconds
   * @param evaluatorName evaluator implementation name
   */
  public EvaluationResult(
      MetricScope scope,
      boolean evaluation,
      Map<String, List<MetricSample>> beforeMetrics,
      Map<String, List<MetricSample>> afterMetrics,
      long actionTimeSeconds,
      long rangeSeconds,
      String evaluatorName) {
    Preconditions.checkArgument(scope != null, "scope must not be null");
    Preconditions.checkArgument(evaluatorName != null, "evaluatorName must not be null");
    this.scope = scope;
    this.evaluation = evaluation;
    this.beforeMetrics = immutableMetrics(beforeMetrics);
    this.afterMetrics = immutableMetrics(afterMetrics);
    this.actionTimeSeconds = actionTimeSeconds;
    this.rangeSeconds = rangeSeconds;
    this.evaluatorName = evaluatorName;
  }

  /**
   * @return evaluated scope (table/partition/job).
   */
  public MetricScope scope() {
    return scope;
  }

  /**
   * @return true if evaluator considers this scope successful.
   */
  public boolean evaluation() {
    return evaluation;
  }

  /**
   * @return immutable metrics map for samples before the action timestamp.
   */
  public Map<String, List<MetricSample>> beforeMetrics() {
    return beforeMetrics;
  }

  /**
   * @return immutable metrics map for samples at/after the action timestamp.
   */
  public Map<String, List<MetricSample>> afterMetrics() {
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

  private static Map<String, List<MetricSample>> immutableMetrics(
      Map<String, List<MetricSample>> metrics) {
    if (metrics == null || metrics.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, List<MetricSample>> copied = new HashMap<>();
    for (Map.Entry<String, List<MetricSample>> entry : metrics.entrySet()) {
      List<MetricSample> samples =
          entry.getValue() == null ? List.of() : new ArrayList<>(entry.getValue());
      copied.put(entry.getKey(), Collections.unmodifiableList(samples));
    }
    return Collections.unmodifiableMap(copied);
  }
}
