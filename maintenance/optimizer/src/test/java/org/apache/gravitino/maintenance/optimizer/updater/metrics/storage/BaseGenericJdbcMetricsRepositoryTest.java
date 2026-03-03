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

package org.apache.gravitino.maintenance.optimizer.updater.metrics.storage;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricScope;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc.GenericJdbcMetricsRepository;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Shared behavior tests for {@link GenericJdbcMetricsRepository}.
 *
 * <p>Environment setup (H2/MySQL/PostgreSQL) is provided by subclasses.
 */
public abstract class BaseGenericJdbcMetricsRepositoryTest {

  protected static final long MAX_REASONABLE_EPOCH_SECONDS = 9_999_999_999L;
  protected GenericJdbcMetricsRepository storage;

  @Test
  void testStoreAndRetrieveTableMetricsWithNullPartition() {
    NameIdentifier nameIdentifier = NameIdentifier.of("catalog", "db", "test_null_partition");
    long now = currentEpochSeconds();
    MetricRecord metric = new MetricRecordImpl(now, "value1");
    MetricRecord metric2 = new MetricRecordImpl(now, "value2");

    storeTableMetric(nameIdentifier, "metric1", Optional.empty(), metric);
    storeTableMetric(nameIdentifier, "metric2", Optional.empty(), metric);
    storeTableMetric(nameIdentifier, "metric2", Optional.empty(), metric2);

    storeTableMetric(
        NameIdentifier.of("catalog2", "db", "test_null_partition"),
        "metric1",
        Optional.empty(),
        metric);
    storeTableMetric(
        NameIdentifier.of("catalog", "db2", "test_null_partition"),
        "metric1",
        Optional.empty(),
        metric);
    storeTableMetric(
        NameIdentifier.of("catalog", "db", "test_null_partition2"),
        "metric1",
        Optional.empty(),
        metric);

    Map<String, List<MetricRecord>> metrics = getTableMetrics(nameIdentifier, 0, Long.MAX_VALUE);

    Assertions.assertEquals(2, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric1"));
    Assertions.assertEquals(Arrays.asList("value1"), getMetricValues(metrics.get("metric1")));
    Assertions.assertTrue(metrics.containsKey("metric2"));
    Assertions.assertEquals(
        Arrays.asList("value1", "value2"), getMetricValues(metrics.get("metric2")));
  }

  @Test
  void testStoreAndRetrieveMetricsWithNonNullPartition() {
    NameIdentifier nameIdentifier = NameIdentifier.of("catalog", "db", "test_non_null_partition");
    long now = currentEpochSeconds();
    MetricRecord metric = new MetricRecordImpl(now, "value1");
    MetricRecord metric2 = new MetricRecordImpl(now, "value2");
    MetricRecord metric3 = new MetricRecordImpl(now, "value3");

    String partition1 = "a=1/b=2";
    String partition2 = "a=1/b=3";

    storeTableMetric(nameIdentifier, "metric", Optional.of(partition1), metric);
    storeTableMetric(nameIdentifier, "metric", Optional.of(partition2), metric);
    storeTableMetric(nameIdentifier, "metric2", Optional.of(partition2), metric2);
    storeTableMetric(nameIdentifier, "metric2", Optional.of(partition2), metric3);

    Map<String, List<MetricRecord>> metrics =
        getPartitionMetrics(nameIdentifier, partition1, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric"));
    Assertions.assertEquals(Arrays.asList("value1"), getMetricValues(metrics.get("metric")));

    metrics = getPartitionMetrics(nameIdentifier, partition2, 0, Long.MAX_VALUE);
    Assertions.assertEquals(2, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric"));
    Assertions.assertEquals(Arrays.asList("value1"), getMetricValues(metrics.get("metric")));
    Assertions.assertTrue(metrics.containsKey("metric2"));
    Assertions.assertEquals(
        Arrays.asList("value2", "value3"), getMetricValues(metrics.get("metric2")));
  }

  @Test
  void testRetrieveMetricsWithNullAndNonNullPartition() {
    NameIdentifier nameIdentifier = NameIdentifier.of("test_mismatch_partition");
    MetricRecord metric = new MetricRecordImpl(0, "value1");
    MetricRecord metric2 = new MetricRecordImpl(1, "value2");
    MetricRecord metric3 = new MetricRecordImpl(2, "value3");

    String partition1 = "a=1/b=2";
    String partition2 = "a=1/b=3";

    storeTableMetric(nameIdentifier, "metric1", Optional.empty(), metric);
    storeTableMetric(nameIdentifier, "metric1", Optional.empty(), metric2);
    storeTableMetric(nameIdentifier, "metric1", Optional.empty(), metric3);
    storeTableMetric(nameIdentifier, "metric1", Optional.of(partition1), metric);
    storeTableMetric(nameIdentifier, "metric1", Optional.of(partition2), metric2);
    storeTableMetric(nameIdentifier, "metric1", Optional.of(partition2), metric3);

    Map<String, List<MetricRecord>> metrics = getTableMetrics(nameIdentifier, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric1"));
    Assertions.assertEquals(
        Arrays.asList("value1", "value2", "value3"), getMetricValues(metrics.get("metric1")));

    metrics = getPartitionMetrics(nameIdentifier, partition1, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric1"));
    Assertions.assertEquals(Arrays.asList("value1"), getMetricValues(metrics.get("metric1")));

    metrics = getPartitionMetrics(nameIdentifier, partition2, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric1"));
    Assertions.assertEquals(
        Arrays.asList("value2", "value3"), getMetricValues(metrics.get("metric1")));
  }

  @Test
  void testCaseInsensitiveIdentifierPartitionAndMetricName() {
    NameIdentifier storedId = NameIdentifier.of("CATALOGX", "DBX", "TABLEX");
    NameIdentifier queryId = NameIdentifier.of("catalogx", "dbx", "tablex");
    MetricRecord metric = new MetricRecordImpl(currentEpochSeconds(), "v1");
    String storedPartition = "Region=US/Day=2025-01-01";
    String queryPartition = "region=us/day=2025-01-01";

    storeTableMetric(storedId, "METRIC_UPPER", Optional.of(storedPartition), metric);
    storeJobMetric(storedId, "JOB_METRIC", metric);

    Map<String, List<MetricRecord>> partitionMetrics =
        getPartitionMetrics(queryId, queryPartition, 0, Long.MAX_VALUE);
    Assertions.assertTrue(partitionMetrics.containsKey("metric_upper"));
    Assertions.assertEquals(List.of("v1"), getMetricValues(partitionMetrics.get("metric_upper")));

    Map<String, List<MetricRecord>> jobMetrics = getJobMetrics(queryId, 0, Long.MAX_VALUE);
    Assertions.assertTrue(jobMetrics.containsKey("job_metric"));
    Assertions.assertEquals(List.of("v1"), getMetricValues(jobMetrics.get("job_metric")));
  }

  @Test
  void testInvalidTimeWindowFailsFast() {
    NameIdentifier id = NameIdentifier.of("catalog", "db", "table");

    IllegalArgumentException tableException =
        Assertions.assertThrows(IllegalArgumentException.class, () -> getTableMetrics(id, 10, 10));
    Assertions.assertTrue(tableException.getMessage().contains("Invalid time window"));

    IllegalArgumentException partitionException =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> getPartitionMetrics(id, "p=1", 20, 10));
    Assertions.assertTrue(partitionException.getMessage().contains("Invalid time window"));

    IllegalArgumentException jobException =
        Assertions.assertThrows(IllegalArgumentException.class, () -> getJobMetrics(id, 30, 30));
    Assertions.assertTrue(jobException.getMessage().contains("Invalid time window"));

    Assertions.assertThrows(IllegalArgumentException.class, () -> getTableMetrics(null, 0, 1));
    Assertions.assertThrows(IllegalArgumentException.class, () -> getJobMetrics(null, 0, 1));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> getPartitionMetrics(null, "p=1", 0, 1));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> getPartitionMetrics(id, " ", 0, 1));
  }

  @Test
  void testWriteValidationFailsFast() {
    MetricRecord metric = new MetricRecordImpl(currentEpochSeconds(), "v1");
    NameIdentifier id = NameIdentifier.of("catalog", "db", "table");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> storeTableMetric(null, "metric", Optional.empty(), metric));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> storeTableMetric(id, " ", Optional.empty(), metric));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> storeTableMetric(id, "metric", Optional.empty(), null));

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> storeJobMetric(null, "metric", metric));
    Assertions.assertThrows(IllegalArgumentException.class, () -> storeJobMetric(id, "", metric));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> storeJobMetric(id, "metric", null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> storeTableMetric(id, "metric", Optional.empty(), new MetricRecordImpl(-1, "v1")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            storeTableMetric(
                id,
                "metric",
                Optional.empty(),
                new MetricRecordImpl(System.currentTimeMillis(), "v1")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> storeJobMetric(id, "metric", new MetricRecordImpl(-1, "v1")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            storeTableMetric(
                id, "metric", Optional.of(" "), new MetricRecordImpl(currentEpochSeconds(), "v1")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            storeTableMetric(
                id, "metric", Optional.empty(), new MetricRecordImpl(currentEpochSeconds(), null)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            storeTableMetric(
                NameIdentifier.of("catalog", "db", "t".repeat(1021)),
                "metric",
                Optional.empty(),
                new MetricRecordImpl(currentEpochSeconds(), "v1")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            storeTableMetric(
                id,
                "m".repeat(1025),
                Optional.empty(),
                new MetricRecordImpl(currentEpochSeconds(), "v1")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            storeTableMetric(
                id,
                "metric",
                Optional.empty(),
                new MetricRecordImpl(currentEpochSeconds(), "x".repeat(1025))));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            storeJobMetric(
                id, "metric", new MetricRecordImpl(currentEpochSeconds(), "x".repeat(1025))));
  }

  @Test
  void testCleanupValidationFailsFast() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> storage.cleanupTableMetricsBefore(-1));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> storage.cleanupJobMetricsBefore(-1));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> storage.cleanupTableMetricsBefore(System.currentTimeMillis()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> storage.cleanupJobMetricsBefore(System.currentTimeMillis()));
  }

  @Test
  void testLongPartitionAndValueAreSupported() {
    NameIdentifier tableId = NameIdentifier.of("catalog", "db", "long_payload_table");
    NameIdentifier jobId = NameIdentifier.of("catalog", "db", "long_payload_job");
    String partition = "p=" + "x".repeat(300);
    String longValue = "v".repeat(1000);
    long now = currentEpochSeconds();

    storeTableMetric(
        tableId, "metric_long", Optional.of(partition), new MetricRecordImpl(now, longValue));
    storeJobMetric(jobId, "metric_long", new MetricRecordImpl(now, longValue));

    Map<String, List<MetricRecord>> tableMetrics =
        getPartitionMetrics(tableId, partition, 0, Long.MAX_VALUE);
    Assertions.assertTrue(tableMetrics.containsKey("metric_long"));
    Assertions.assertEquals(longValue, tableMetrics.get("metric_long").get(0).getValue());

    Map<String, List<MetricRecord>> jobMetrics = getJobMetrics(jobId, 0, Long.MAX_VALUE);
    Assertions.assertTrue(jobMetrics.containsKey("metric_long"));
    Assertions.assertEquals(longValue, jobMetrics.get("metric_long").get(0).getValue());
  }

  protected List<String> getMetricValues(List<MetricRecord> metrics) {
    return metrics.stream().map(MetricRecord::getValue).toList();
  }

  protected Map<String, List<MetricRecord>> getTableMetrics(
      NameIdentifier nameIdentifier, long fromSecs, long toSecs) {
    return convertToMetricRecords(
        storage.getMetrics(MetricScope.forTable(nameIdentifier), fromSecs, toSecs));
  }

  protected Map<String, List<MetricRecord>> getPartitionMetrics(
      NameIdentifier nameIdentifier, String partition, long fromSecs, long toSecs) {
    return convertToMetricRecords(
        storage.getMetrics(
            MetricScope.forPartition(nameIdentifier, parsePartitionPath(partition)),
            fromSecs,
            toSecs));
  }

  protected Map<String, List<MetricRecord>> getJobMetrics(
      NameIdentifier nameIdentifier, long fromSecs, long toSecs) {
    return convertToMetricRecords(
        storage.getMetrics(MetricScope.forJob(nameIdentifier), fromSecs, toSecs));
  }

  protected void storeTableMetric(
      NameIdentifier nameIdentifier,
      String metricName,
      Optional<String> partition,
      MetricRecord metric) {
    MetricPoint metricPoint;
    if (metric == null) {
      metricPoint = null;
    } else if (partition != null && partition.isPresent()) {
      metricPoint =
          MetricPoint.forPartition(
              nameIdentifier,
              parsePartitionPath(partition.get()),
              metricName,
              toStatisticValue(metric.getValue()),
              metric.getTimestamp());
    } else {
      metricPoint =
          MetricPoint.forTable(
              nameIdentifier,
              metricName,
              toStatisticValue(metric.getValue()),
              metric.getTimestamp());
    }
    storage.storeMetrics(List.of(metricPoint));
  }

  protected void storeJobMetric(
      NameIdentifier nameIdentifier, String metricName, MetricRecord metric) {
    MetricPoint metricPoint =
        metric == null
            ? null
            : MetricPoint.forJob(
                nameIdentifier,
                metricName,
                toStatisticValue(metric.getValue()),
                metric.getTimestamp());
    storage.storeMetrics(List.of(metricPoint));
  }

  protected Map<String, List<MetricRecord>> convertToMetricRecords(List<MetricPoint> metricPoints) {
    Map<String, List<MetricRecord>> metrics = new LinkedHashMap<>();
    for (MetricPoint metricPoint : metricPoints) {
      MetricRecord metricRecord =
          new MetricRecordImpl(
              metricPoint.timestampSeconds(), String.valueOf(metricPoint.value().value()));
      metrics
          .computeIfAbsent(metricPoint.metricName(), ignored -> new ArrayList<>())
          .add(metricRecord);
    }
    return metrics;
  }

  protected PartitionPath parsePartitionPath(String partition) {
    if (partition == null || partition.trim().isEmpty()) {
      throw new IllegalArgumentException("partition must not be blank");
    }
    String[] entries = partition.split("/");
    List<PartitionEntry> partitionEntries = new ArrayList<>(entries.length);
    for (String entry : entries) {
      String[] kv = entry.split("=", 2);
      if (kv.length != 2 || kv[0].trim().isEmpty() || kv[1].trim().isEmpty()) {
        throw new IllegalArgumentException("Invalid partition entry: " + entry);
      }
      partitionEntries.add(new PartitionEntryImpl(kv[0], kv[1]));
    }
    return PartitionPath.of(partitionEntries);
  }

  private StatisticValue<?> toStatisticValue(String value) {
    return value == null ? null : StatisticValues.stringValue(value);
  }

  protected long currentEpochSeconds() {
    return Instant.now().getEpochSecond();
  }
}
