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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prometheus.client.Collector;
import io.prometheus.client.dropwizard.samplebuilder.MapperConfig;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestGravitinoSampleBuilder {

  private static GravitinoSampleBuilder sampleBuilder;

  @BeforeAll
  public static void setUp() {
    List<MapperConfig> mapperConfigs =
        Collections.singletonList(
            new MapperConfig(
                "test.default.*", "test_default_${0}", ImmutableMap.of("name", "${0}")));
    sampleBuilder = new GravitinoSampleBuilder(mapperConfigs);
  }

  @Test
  public void testCreateSampleWithCatalogMetric() {
    String dropwizardName = "gravitino-catalog.hive.metalake1.catalog1.some.metric.count";
    double value = 10.0;

    Collector.MetricFamilySamples.Sample sample =
        sampleBuilder.createSample(
            dropwizardName, "", Collections.emptyList(), Collections.emptyList(), value);

    Assertions.assertEquals("gravitino_catalog_some_metric_count", sample.name);
    Assertions.assertEquals(ImmutableList.of("provider", "metalake", "catalog"), sample.labelNames);
    Assertions.assertEquals(ImmutableList.of("hive", "metalake1", "catalog1"), sample.labelValues);
    Assertions.assertEquals(value, sample.value);
  }

  @Test
  public void testCreateSampleWithCatalogMetricAndAdditionalLabels() {
    String dropwizardName = "gravitino-catalog.jdbc.metalake2.catalog2.another.metric";
    double value = 20.0;
    List<String> additionalLabelNames = Collections.singletonList("type");
    List<String> additionalLabelValues = Collections.singletonList("gauge");

    Collector.MetricFamilySamples.Sample sample =
        sampleBuilder.createSample(
            dropwizardName, "", additionalLabelNames, additionalLabelValues, value);

    Assertions.assertEquals("gravitino_catalog_another_metric", sample.name);
    Assertions.assertEquals(
        ImmutableList.of("provider", "metalake", "catalog", "type"), sample.labelNames);
    Assertions.assertEquals(
        ImmutableList.of("jdbc", "metalake2", "catalog2", "gauge"), sample.labelValues);
    Assertions.assertEquals(value, sample.value);
  }

  @Test
  public void testCreateSampleWithNonCatalogMetricMatchingParentRule() {
    String dropwizardName = "test.default.metric1";
    double value = 30.0;

    Collector.MetricFamilySamples.Sample sample =
        sampleBuilder.createSample(
            dropwizardName, "", Collections.emptyList(), Collections.emptyList(), value);

    Assertions.assertEquals("test_default_metric1", sample.name);
    Assertions.assertEquals(Collections.singletonList("name"), sample.labelNames);
    Assertions.assertEquals(Collections.singletonList("metric1"), sample.labelValues);
    Assertions.assertEquals(value, sample.value);
  }

  @Test
  public void testCreateSampleWithNonCatalogMetricNotMatchingAnyRule() {
    String dropwizardName = "unmatched.metric.name";
    double value = 40.0;

    Collector.MetricFamilySamples.Sample sample =
        sampleBuilder.createSample(
            dropwizardName, "", Collections.emptyList(), Collections.emptyList(), value);

    Assertions.assertEquals("unmatched_metric_name", sample.name);
    Assertions.assertTrue(sample.labelNames.isEmpty());
    Assertions.assertTrue(sample.labelValues.isEmpty());
    Assertions.assertEquals(value, sample.value);
  }

  @Test
  public void testCreateSampleWithHyphensInMetricNameRest() {
    String dropwizardName = "gravitino-catalog.fileset.metalake1.catalog1.schema-cache.hit-count";
    double value = 50.0;

    Collector.MetricFamilySamples.Sample sample =
        sampleBuilder.createSample(
            dropwizardName, "", Collections.emptyList(), Collections.emptyList(), value);

    // Both prefix and metricNameRest should have hyphens converted to underscores
    Assertions.assertEquals("gravitino_catalog_schema_cache_hit_count", sample.name);
    Assertions.assertEquals(ImmutableList.of("provider", "metalake", "catalog"), sample.labelNames);
    Assertions.assertEquals(
        ImmutableList.of("fileset", "metalake1", "catalog1"), sample.labelValues);
    Assertions.assertEquals(value, sample.value);
  }

  @Test
  public void testCreateSampleWithFilesetCacheMetric() {
    String dropwizardName =
        "gravitino-catalog.fileset.test-metalake.test-catalog.fileset-cache.miss-count";
    double value = 15.0;

    Collector.MetricFamilySamples.Sample sample =
        sampleBuilder.createSample(
            dropwizardName, "", Collections.emptyList(), Collections.emptyList(), value);

    // All hyphens should be converted to underscores
    Assertions.assertEquals("gravitino_catalog_fileset_cache_miss_count", sample.name);
    Assertions.assertEquals(ImmutableList.of("provider", "metalake", "catalog"), sample.labelNames);
    Assertions.assertEquals(
        ImmutableList.of("fileset", "test-metalake", "test-catalog"), sample.labelValues);
    Assertions.assertEquals(value, sample.value);
  }

  @Test
  public void testCreateSampleWithMultipleHyphensAndDots() {
    String dropwizardName =
        "gravitino-catalog.hive.my-metalake.my-catalog.complex-metric-name.sub-component.total";
    double value = 100.0;

    Collector.MetricFamilySamples.Sample sample =
        sampleBuilder.createSample(
            dropwizardName, "", Collections.emptyList(), Collections.emptyList(), value);

    // All dots and hyphens in metricNameRest should be converted to underscores
    Assertions.assertEquals(
        "gravitino_catalog_complex_metric_name_sub_component_total", sample.name);
    Assertions.assertEquals(ImmutableList.of("provider", "metalake", "catalog"), sample.labelNames);
    Assertions.assertEquals(
        ImmutableList.of("hive", "my-metalake", "my-catalog"), sample.labelValues);
    Assertions.assertEquals(value, sample.value);
  }

  @Test
  public void testCreateSampleWithOnlyHyphensInMetricName() {
    String dropwizardName = "gravitino-catalog.jdbc.metalake.catalog.http-request-duration";
    double value = 0.5;

    Collector.MetricFamilySamples.Sample sample =
        sampleBuilder.createSample(
            dropwizardName, "", Collections.emptyList(), Collections.emptyList(), value);

    // Hyphens in metricNameRest should be converted to underscores
    Assertions.assertEquals("gravitino_catalog_http_request_duration", sample.name);
    Assertions.assertEquals(ImmutableList.of("provider", "metalake", "catalog"), sample.labelNames);
    Assertions.assertEquals(ImmutableList.of("jdbc", "metalake", "catalog"), sample.labelValues);
    Assertions.assertEquals(value, sample.value);
  }

  @Test
  public void testCreateSampleWithMixedSpecialCharacters() {
    String dropwizardName = "gravitino-catalog.model.test.example.cache-stats.hit-ratio.avg";
    double value = 0.85;

    Collector.MetricFamilySamples.Sample sample =
        sampleBuilder.createSample(
            dropwizardName, "", Collections.emptyList(), Collections.emptyList(), value);

    // Both dots and hyphens should be converted to underscores
    Assertions.assertEquals("gravitino_catalog_cache_stats_hit_ratio_avg", sample.name);
    Assertions.assertEquals(ImmutableList.of("provider", "metalake", "catalog"), sample.labelNames);
    Assertions.assertEquals(ImmutableList.of("model", "test", "example"), sample.labelValues);
    Assertions.assertEquals(value, sample.value);
  }

  @Test
  public void testCreateSampleWithAdditionalLabelsAndHyphens() {
    String dropwizardName =
        "gravitino-catalog.fileset.prod-metalake.main-catalog.schema-cache.eviction-count";
    double value = 25.0;
    List<String> additionalLabelNames = ImmutableList.of("cache-type", "operation-mode");
    List<String> additionalLabelValues = ImmutableList.of("lru", "async");

    Collector.MetricFamilySamples.Sample sample =
        sampleBuilder.createSample(
            dropwizardName, "", additionalLabelNames, additionalLabelValues, value);

    // Metric name should have all hyphens converted to underscores
    Assertions.assertEquals("gravitino_catalog_schema_cache_eviction_count", sample.name);
    Assertions.assertEquals(
        ImmutableList.of("provider", "metalake", "catalog", "cache-type", "operation-mode"),
        sample.labelNames);
    Assertions.assertEquals(
        ImmutableList.of("fileset", "prod-metalake", "main-catalog", "lru", "async"),
        sample.labelValues);
    Assertions.assertEquals(value, sample.value);
  }
}
