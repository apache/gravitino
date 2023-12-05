/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.metrics;

import com.datastrato.gravitino.metrics.source.MetricsSource;
import com.google.common.collect.ImmutableMap;
import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.dropwizard.samplebuilder.CustomMappingSampleBuilder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
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
  }
}
