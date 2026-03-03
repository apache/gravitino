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

package org.apache.gravitino.maintenance.optimizer.monitor.callback;

import java.time.Instant;
import java.util.stream.Collectors;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSeries;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricValueSample;
import org.apache.gravitino.maintenance.optimizer.api.monitor.EvaluationResult;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MonitorCallback;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;

/** Built-in callback that prints evaluation results and metric summaries to console. */
public class ConsoleMonitorCallback implements MonitorCallback {

  public static final String NAME = "console";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {}

  @Override
  public void onEvaluation(EvaluationResult result) {
    System.out.println(
        String.format(
            "MONITOR: time=%s scope=%s identifier=%s%s evaluation=%s evaluator=%s",
            Instant.now(),
            result.scope().type(),
            result.scope().identifier(),
            result.scope().partition().isPresent()
                ? " partition=" + result.scope().partition().get()
                : "",
            result.evaluation(),
            result.evaluatorName()));
    if (!result.beforeSeries().isEmpty()) {
      System.out.println("METRICS BEFORE: " + formatMetrics(result.beforeSeries()));
    }
    if (!result.afterSeries().isEmpty()) {
      System.out.println("METRICS AFTER: " + formatMetrics(result.afterSeries()));
    }
  }

  @Override
  public void close() throws Exception {}

  private String formatMetrics(MetricSeries metricSeries) {
    if (metricSeries == null || metricSeries.isEmpty()) {
      return "[]";
    }
    return metricSeries.samplesByMetricName().entrySet().stream()
        .map(
            entry ->
                entry.getKey()
                    + "="
                    + entry.getValue().stream()
                        .map(this::formatMetricSample)
                        .collect(Collectors.joining(", ", "[", "]")))
        .collect(Collectors.joining(", ", "{", "}"));
  }

  private String formatMetricSample(MetricValueSample sample) {
    if (sample == null || sample.value() == null) {
      return "N/A";
    }
    return String.format(
        "%d:%s", sample.timestampSeconds(), String.valueOf(sample.value().value()));
  }
}
