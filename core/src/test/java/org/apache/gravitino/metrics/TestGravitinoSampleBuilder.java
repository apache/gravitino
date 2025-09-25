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

    Assertions.assertEquals(GRAVITINO_CATALOG_METRIC_PREFIX + "_some_metric_count", sample.name);
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

    Assertions.assertEquals(GRAVITINO_CATALOG_METRIC_PREFIX + "_another_metric", sample.name);
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
}
