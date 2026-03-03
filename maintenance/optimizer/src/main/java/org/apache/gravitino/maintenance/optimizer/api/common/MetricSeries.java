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

package org.apache.gravitino.maintenance.optimizer.api.common;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricScope;
import org.apache.gravitino.maintenance.optimizer.common.util.MetricScopePointValidator;

/**
 * Immutable metric series for one scope, grouped by metric name into samples.
 *
 * <p>This class is an evaluator-facing view derived from {@link MetricPoint}.
 */
@DeveloperApi
public final class MetricSeries {

  private final MetricScope scope;
  private final Map<String, List<MetricValueSample>> samplesByMetricName;

  private MetricSeries(
      MetricScope scope, Map<String, List<MetricValueSample>> samplesByMetricName) {
    Preconditions.checkArgument(scope != null, "scope must not be null");
    Preconditions.checkArgument(
        samplesByMetricName != null, "samplesByMetricName must not be null");
    this.scope = scope;
    this.samplesByMetricName = samplesByMetricName;
  }

  /** Build one scope series from metric points and validate each point belongs to the scope. */
  public static MetricSeries fromPoints(MetricScope scope, List<MetricPoint> points) {
    Preconditions.checkArgument(scope != null, "scope must not be null");
    if (points == null || points.isEmpty()) {
      return new MetricSeries(scope, Collections.emptyMap());
    }

    Map<String, List<MetricValueSample>> grouped = new LinkedHashMap<>();
    for (MetricPoint point : points) {
      Optional<String> invalidReason = MetricScopePointValidator.invalidReason(scope, point);
      Preconditions.checkArgument(
          invalidReason.isEmpty(),
          "Metric point does not belong to scope type=%s, identifier=%s, reason=%s",
          scope.type(),
          scope.identifier(),
          invalidReason.orElse("unknown"));
      String normalizedMetricName = normalizeMetricName(point.metricName());
      grouped
          .computeIfAbsent(normalizedMetricName, ignored -> new ArrayList<>())
          .add(new MetricValueSample(point.timestampSeconds(), point.value()));
    }

    return ofSamples(scope, grouped);
  }

  /** Build one scope series from grouped samples. */
  public static MetricSeries ofSamples(
      MetricScope scope, Map<String, List<MetricValueSample>> samplesByMetricName) {
    Preconditions.checkArgument(scope != null, "scope must not be null");
    if (samplesByMetricName == null || samplesByMetricName.isEmpty()) {
      return new MetricSeries(scope, Collections.emptyMap());
    }

    Map<String, List<MetricValueSample>> mergedMap = new LinkedHashMap<>();
    for (Map.Entry<String, List<MetricValueSample>> entry : samplesByMetricName.entrySet()) {
      String metricName = normalizeMetricName(entry.getKey());
      List<MetricValueSample> samples = entry.getValue();
      Preconditions.checkArgument(
          samples != null, "samples must not be null for metric %s", metricName);
      List<MetricValueSample> mergedSamples =
          mergedMap.computeIfAbsent(metricName, ignored -> new ArrayList<>());
      for (MetricValueSample sample : samples) {
        Preconditions.checkArgument(
            sample != null, "sample must not be null for metric %s", metricName);
        mergedSamples.add(sample);
      }
    }

    Map<String, List<MetricValueSample>> immutableMap = new LinkedHashMap<>();
    for (Map.Entry<String, List<MetricValueSample>> entry : mergedMap.entrySet()) {
      immutableMap.put(
          entry.getKey(), Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
    }
    return new MetricSeries(scope, Collections.unmodifiableMap(immutableMap));
  }

  public MetricScope scope() {
    return scope;
  }

  public Map<String, List<MetricValueSample>> samplesByMetricName() {
    return samplesByMetricName;
  }

  public List<MetricValueSample> samples(String metricName) {
    return samplesByMetricName.getOrDefault(normalizeMetricName(metricName), List.of());
  }

  public boolean isEmpty() {
    return samplesByMetricName.isEmpty();
  }

  private static String normalizeMetricName(String metricName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(metricName), "metricName must not be blank");
    return metricName.trim().toLowerCase(Locale.ROOT);
  }
}
