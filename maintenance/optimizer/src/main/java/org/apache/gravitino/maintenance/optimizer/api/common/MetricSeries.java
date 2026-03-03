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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricScope;

/**
 * Immutable metric series for one scope, grouped by metric name into ordered samples.
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
      Preconditions.checkArgument(point != null, "metric point must not be null");
      validatePointBelongsToScope(scope, point);
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

    Map<String, List<MetricValueSample>> immutableMap = new LinkedHashMap<>();
    for (Map.Entry<String, List<MetricValueSample>> entry : samplesByMetricName.entrySet()) {
      String metricName = normalizeMetricName(entry.getKey());
      List<MetricValueSample> samples = entry.getValue();
      Preconditions.checkArgument(
          samples != null, "samples must not be null for metric %s", metricName);
      List<MetricValueSample> sortedSamples = new ArrayList<>(samples.size());
      for (MetricValueSample sample : samples) {
        Preconditions.checkArgument(
            sample != null, "sample must not be null for metric %s", metricName);
        sortedSamples.add(sample);
      }
      sortedSamples.sort(Comparator.comparingLong(MetricValueSample::timestampSeconds));
      immutableMap.put(metricName, Collections.unmodifiableList(sortedSamples));
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

  private static void validatePointBelongsToScope(MetricScope scope, MetricPoint point) {
    NameIdentifier expectedIdentifier = scope.identifier();
    Preconditions.checkArgument(
        expectedIdentifier.equals(point.identifier()),
        "Metric point identifier %s does not match scope identifier %s",
        point.identifier(),
        expectedIdentifier);

    switch (scope.type()) {
      case TABLE:
        Preconditions.checkArgument(
            point.scope() == MetricPoint.Scope.TABLE,
            "Metric point scope %s does not match TABLE scope",
            point.scope());
        Preconditions.checkArgument(
            !point.partitionPath().isPresent(),
            "TABLE scope point must not contain partition path");
        return;
      case PARTITION:
        Preconditions.checkArgument(
            point.scope() == MetricPoint.Scope.PARTITION,
            "Metric point scope %s does not match PARTITION scope",
            point.scope());
        Preconditions.checkArgument(
            scope.partition().isPresent(), "PARTITION scope must contain partition path");
        Preconditions.checkArgument(
            point.partitionPath().isPresent(), "PARTITION scope point must contain partition path");
        Preconditions.checkArgument(
            Objects.equals(scope.partition().get(), point.partitionPath().get()),
            "Metric point partition path %s does not match scope partition path %s",
            point.partitionPath().get(),
            scope.partition().get());
        return;
      case JOB:
        Preconditions.checkArgument(
            point.scope() == MetricPoint.Scope.JOB,
            "Metric point scope %s does not match JOB scope",
            point.scope());
        Preconditions.checkArgument(
            !point.partitionPath().isPresent(), "JOB scope point must not contain partition path");
        return;
      default:
        throw new IllegalArgumentException("Unsupported metric scope type: " + scope.type());
    }
  }

  private static String normalizeMetricName(String metricName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(metricName), "metricName must not be blank");
    return metricName.trim().toLowerCase(Locale.ROOT);
  }
}
