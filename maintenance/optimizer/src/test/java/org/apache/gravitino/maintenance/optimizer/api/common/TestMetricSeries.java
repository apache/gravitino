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

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricScope;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestMetricSeries {

  @Test
  void testOfSamplesMergesKeysAfterNormalization() {
    NameIdentifier identifier = NameIdentifier.parse("catalog.db.table");
    MetricScope scope = MetricScope.forTable(identifier);
    MetricSeries series =
        MetricSeries.ofSamples(
            scope,
            Map.of(
                "Row_Count",
                List.of(new MetricValueSample(101L, StatisticValues.longValue(2L))),
                "row_count",
                List.of(new MetricValueSample(100L, StatisticValues.longValue(1L)))));

    Assertions.assertEquals(1, series.samplesByMetricName().size());
    Assertions.assertTrue(series.samplesByMetricName().containsKey("row_count"));
    Assertions.assertEquals(
        List.of(100L, 101L),
        series.samples("row_count").stream().map(MetricValueSample::timestampSeconds).toList());
  }

  @Test
  void testFromPointsGroupsSortsAndNormalizesMetricName() {
    NameIdentifier identifier = NameIdentifier.parse("catalog.db.table");
    MetricScope scope = MetricScope.forTable(identifier);
    List<MetricPoint> points =
        List.of(
            MetricPoint.forTable(identifier, "Row_Count", StatisticValues.longValue(2L), 101L),
            MetricPoint.forTable(identifier, "row_count", StatisticValues.longValue(1L), 100L),
            MetricPoint.forTable(identifier, "file_count", StatisticValues.longValue(3L), 99L));

    MetricSeries metricSeries = MetricSeries.fromPoints(scope, points);

    Assertions.assertEquals(2, metricSeries.samplesByMetricName().size());
    Assertions.assertTrue(metricSeries.samplesByMetricName().containsKey("row_count"));
    Assertions.assertEquals(
        List.of(100L, 101L),
        metricSeries.samples("row_count").stream()
            .map(MetricValueSample::timestampSeconds)
            .toList());
  }

  @Test
  void testFromPointsRejectsIdentifierMismatch() {
    NameIdentifier scopeIdentifier = NameIdentifier.parse("catalog.db.table");
    NameIdentifier metricIdentifier = NameIdentifier.parse("catalog.db.other_table");
    MetricScope scope = MetricScope.forTable(scopeIdentifier);
    List<MetricPoint> points =
        List.of(
            MetricPoint.forTable(
                metricIdentifier, "row_count", StatisticValues.longValue(1L), 100L));

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> MetricSeries.fromPoints(scope, points));
  }

  @Test
  void testFromPointsRejectsPartitionMismatch() {
    NameIdentifier identifier = NameIdentifier.parse("catalog.db.table");
    PartitionPath scopePartition =
        PartitionPath.of(List.of(new PartitionEntryImpl("dt", "2026-03-02")));
    PartitionPath metricPartition =
        PartitionPath.of(List.of(new PartitionEntryImpl("dt", "2026-03-01")));
    MetricScope scope = MetricScope.forPartition(identifier, scopePartition);
    List<MetricPoint> points =
        List.of(
            MetricPoint.forPartition(
                identifier, metricPartition, "row_count", StatisticValues.longValue(1L), 100L));

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> MetricSeries.fromPoints(scope, points));
  }
}
