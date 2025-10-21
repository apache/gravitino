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
package org.apache.gravitino.metrics;

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_CATALOG_METRIC_PREFIX;

import io.prometheus.client.Collector;
import io.prometheus.client.dropwizard.samplebuilder.CustomMappingSampleBuilder;
import io.prometheus.client.dropwizard.samplebuilder.MapperConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * GravitinoSampleBuilder is a custom SampleBuilder for Prometheus that transforms Dropwizard
 * metrics into Prometheus samples. It is designed to handle Gravitino's metric naming conventions
 * for catalog-level metrics.
 *
 * <p>The metric names are expected to follow a specific format to be processed correctly. The
 * format is: {@code gravitino-catalog.<provider>.<metalake>.<catalog>.<rest-of-the-metric-name>}.
 *
 * <p>This builder extracts the following labels from the metric name:
 *
 * <ul>
 *   <li>{@code provider}: The provider of the catalog (e.g., fileset, model, hive).
 *   <li>{@code metalake}: The name of the metalake.
 *   <li>{@code catalog}: The name of the catalog.
 * </ul>
 *
 * <p>For example, a Dropwizard metric named {@code
 * gravitino-catalog.fileset.metalake1.catalog1.some-metric} will be converted to a Prometheus
 * sample with the metric name {@code gravitino_catalog_some_metric} and the labels {@code
 * provider="fileset"}, {@code metalake="metalake1"}, and {@code catalog="catalog1"}.
 *
 * <p>We are not using the default {@link CustomMappingSampleBuilder} with {@code MapperConfig}
 * directly because {@code MapperConfig} only supports exact matching of metric names. For metrics
 * that contain multiple variable parts, it would require defining a separate complex rule for each
 * possible combination, which is not practical. This class uses regular expressions to dynamically
 * parse metric names, providing a more flexible and scalable solution.
 *
 * <p>Metrics that do not match this pattern will be processed by the default behavior of {@link
 * CustomMappingSampleBuilder}.
 */
public class GravitinoSampleBuilder extends CustomMappingSampleBuilder {
  // match pattern:
  // gravitino-catalog.<provider>.<metalake>.<catalog>.<rest-of-the-metric-name>
  private static final Pattern CATALOG_PATTERN =
      Pattern.compile(
          Pattern.quote(GRAVITINO_CATALOG_METRIC_PREFIX) + "\\.([^.]+)\\.([^.]+)\\.([^.]+)\\.(.+)");

  public GravitinoSampleBuilder(List<MapperConfig> mapperConfigs) {
    super(mapperConfigs);
  }

  @Override
  public Collector.MetricFamilySamples.Sample createSample(
      String dropwizardName,
      String nameSuffix,
      List<String> additionalLabelNames,
      List<String> additionalLabelValues,
      double value) {

    Matcher matcher = CATALOG_PATTERN.matcher(dropwizardName);
    if (matcher.matches()) {
      String provider = matcher.group(1);
      String metalake = matcher.group(2);
      String catalog = matcher.group(3);
      // Use Prometheus client's sanitization to conform to naming conventions
      String metricNameRest = Collector.sanitizeMetricName(matcher.group(4));
      String sanitizedPrefix = Collector.sanitizeMetricName(GRAVITINO_CATALOG_METRIC_PREFIX);
      String prometheusName = sanitizedPrefix + "_" + metricNameRest;

      List<String> labelNames = new ArrayList<>();
      labelNames.add("provider");
      labelNames.add("metalake");
      labelNames.add("catalog");
      labelNames.addAll(additionalLabelNames);

      List<String> labelValues = new ArrayList<>();
      labelValues.add(provider);
      labelValues.add(metalake);
      labelValues.add(catalog);
      labelValues.addAll(additionalLabelValues);

      return new Collector.MetricFamilySamples.Sample(
          prometheusName, labelNames, labelValues, value);
    }

    // Fallback to the parent class's default behavior for mismatched metrics
    return super.createSample(
        dropwizardName, nameSuffix, additionalLabelNames, additionalLabelValues, value);
  }
}
