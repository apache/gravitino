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

import com.google.common.collect.ImmutableMap;
import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.dropwizard.samplebuilder.CustomMappingSampleBuilder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import org.apache.gravitino.metrics.source.MetricsSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestExtractMetricNameAndLabel {

  CustomMappingSampleBuilder sampleBuilder =
      new CustomMappingSampleBuilder(MetricsSystem.getMetricNameAndLabelRules());

  private void checkResult(String dropwizardName, String metricName, Map<String, String> labels) {
    Sample sample =
        sampleBuilder.createSample(dropwizardName, "", Arrays.asList(), Arrays.asList(), 0);

    Assertions.assertEquals(metricName, sample.name);
    Assertions.assertEquals(labels.keySet(), new HashSet(sample.labelNames));
    Assertions.assertEquals(new HashSet(labels.values()), new HashSet(sample.labelValues));
  }

  @Test
  void testMapperConfig() {
    checkResult("jvm.total.used", "jvm_total_used", ImmutableMap.of());

    checkResult(
        MetricsSource.ICEBERG_REST_SERVER_METRIC_NAME + "." + MetricNames.SERVER_IDLE_THREAD_NUM,
        Collector.sanitizeMetricName(MetricsSource.ICEBERG_REST_SERVER_METRIC_NAME)
            + "_"
            + Collector.sanitizeMetricName(MetricNames.SERVER_IDLE_THREAD_NUM),
        ImmutableMap.of());

    checkResult(
        MetricsSource.GRAVITINO_SERVER_METRIC_NAME + "." + MetricNames.SERVER_IDLE_THREAD_NUM,
        Collector.sanitizeMetricName(MetricsSource.GRAVITINO_SERVER_METRIC_NAME)
            + "_"
            + Collector.sanitizeMetricName(MetricNames.SERVER_IDLE_THREAD_NUM),
        ImmutableMap.of());

    checkResult(
        MetricsSource.ICEBERG_REST_SERVER_METRIC_NAME
            + ".update-table."
            + MetricNames.HTTP_PROCESS_DURATION,
        Collector.sanitizeMetricName(MetricsSource.ICEBERG_REST_SERVER_METRIC_NAME)
            + "_"
            + Collector.sanitizeMetricName(MetricNames.HTTP_PROCESS_DURATION),
        ImmutableMap.of("operation", "update-table"));

    checkResult(
        MetricsSource.GRAVITINO_SERVER_METRIC_NAME
            + ".update-table."
            + MetricNames.HTTP_PROCESS_DURATION,
        Collector.sanitizeMetricName(MetricsSource.GRAVITINO_SERVER_METRIC_NAME)
            + "_"
            + Collector.sanitizeMetricName(MetricNames.HTTP_PROCESS_DURATION),
        ImmutableMap.of("operation", "update-table"));

    checkResult(
        MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME
            + "."
            + MetricNames.DATASOURCE_ACTIVE_CONNECTIONS,
        Collector.sanitizeMetricName(MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME)
            + "_"
            + Collector.sanitizeMetricName(MetricNames.DATASOURCE_ACTIVE_CONNECTIONS),
        ImmutableMap.of());

    checkResult(
        MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME
            + "."
            + MetricNames.DATASOURCE_IDLE_CONNECTIONS,
        Collector.sanitizeMetricName(MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME)
            + "_"
            + Collector.sanitizeMetricName(MetricNames.DATASOURCE_IDLE_CONNECTIONS),
        ImmutableMap.of());

    checkResult(
        MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME
            + "."
            + MetricNames.DATASOURCE_MAX_CONNECTIONS,
        Collector.sanitizeMetricName(MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME)
            + "_"
            + Collector.sanitizeMetricName(MetricNames.DATASOURCE_MAX_CONNECTIONS),
        ImmutableMap.of());
  }
}
