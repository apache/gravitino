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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc.GenericJdbcMetricsRepository;
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

    Map<String, List<MetricRecord>> metrics =
        storage.getTableMetrics(nameIdentifier, 0, Long.MAX_VALUE);

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
        storage.getPartitionMetrics(nameIdentifier, partition1, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric"));
    Assertions.assertEquals(Arrays.asList("value1"), getMetricValues(metrics.get("metric")));

    metrics = storage.getPartitionMetrics(nameIdentifier, partition2, 0, Long.MAX_VALUE);
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

    Map<String, List<MetricRecord>> metrics =
        storage.getTableMetrics(nameIdentifier, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric1"));
    Assertions.assertEquals(
        Arrays.asList("value1", "value2", "value3"), getMetricValues(metrics.get("metric1")));

    metrics = storage.getPartitionMetrics(nameIdentifier, partition1, 0, Long.MAX_VALUE);
    Assertions.assertEquals(1, metrics.size());
    Assertions.assertTrue(metrics.containsKey("metric1"));
    Assertions.assertEquals(Arrays.asList("value1"), getMetricValues(metrics.get("metric1")));

    metrics = storage.getPartitionMetrics(nameIdentifier, partition2, 0, Long.MAX_VALUE);
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
        storage.getPartitionMetrics(queryId, queryPartition, 0, Long.MAX_VALUE);
    Assertions.assertTrue(partitionMetrics.containsKey("metric_upper"));
    Assertions.assertEquals(List.of("v1"), getMetricValues(partitionMetrics.get("metric_upper")));

    Map<String, List<MetricRecord>> jobMetrics = storage.getJobMetrics(queryId, 0, Long.MAX_VALUE);
    Assertions.assertTrue(jobMetrics.containsKey("job_metric"));
    Assertions.assertEquals(List.of("v1"), getMetricValues(jobMetrics.get("job_metric")));
  }

  @Test
  void testInvalidTimeWindowFailsFast() {
    NameIdentifier id = NameIdentifier.of("catalog", "db", "table");

    IllegalArgumentException tableException =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> storage.getTableMetrics(id, 10, 10));
    Assertions.assertTrue(tableException.getMessage().contains("Invalid time window"));

    IllegalArgumentException partitionException =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> storage.getPartitionMetrics(id, "p=1", 20, 10));
    Assertions.assertTrue(partitionException.getMessage().contains("Invalid time window"));

    IllegalArgumentException jobException =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> storage.getJobMetrics(id, 30, 30));
    Assertions.assertTrue(jobException.getMessage().contains("Invalid time window"));

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> storage.getTableMetrics(null, 0, 1));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> storage.getJobMetrics(null, 0, 1));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> storage.getPartitionMetrics(null, "p=1", 0, 1));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> storage.getPartitionMetrics(id, " ", 0, 1));
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
        storage.getPartitionMetrics(tableId, partition, 0, Long.MAX_VALUE);
    Assertions.assertTrue(tableMetrics.containsKey("metric_long"));
    Assertions.assertEquals(longValue, tableMetrics.get("metric_long").get(0).getValue());

    Map<String, List<MetricRecord>> jobMetrics = storage.getJobMetrics(jobId, 0, Long.MAX_VALUE);
    Assertions.assertTrue(jobMetrics.containsKey("metric_long"));
    Assertions.assertEquals(longValue, jobMetrics.get("metric_long").get(0).getValue());
  }

  protected List<String> getMetricValues(List<MetricRecord> metrics) {
    return metrics.stream().map(MetricRecord::getValue).toList();
  }

  protected void storeTableMetric(
      NameIdentifier nameIdentifier,
      String metricName,
      Optional<String> partition,
      MetricRecord metric) {
    storage.storeTableMetrics(
        List.of(new TableMetricWriteRequest(nameIdentifier, metricName, partition, metric)));
  }

  protected void storeJobMetric(
      NameIdentifier nameIdentifier, String metricName, MetricRecord metric) {
    storage.storeJobMetrics(List.of(new JobMetricWriteRequest(nameIdentifier, metricName, metric)));
  }

  protected long currentEpochSeconds() {
    return Instant.now().getEpochSecond();
  }
}
