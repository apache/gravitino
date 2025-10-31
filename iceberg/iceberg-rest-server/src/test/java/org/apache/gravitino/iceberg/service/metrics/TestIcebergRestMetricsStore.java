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
package org.apache.gravitino.iceberg.service.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.metrics.source.IcebergClientMetricsSource;
import org.apache.iceberg.metrics.ImmutableCommitMetricsResult;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.ImmutableCounterResult;
import org.apache.iceberg.metrics.ImmutableTimerResult;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.metrics.MetricsReport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIcebergRestMetricsStore {

  private IcebergRestMetricsStore store;
  private IcebergClientMetricsSource metricsSource;

  @BeforeEach
  public void setUp() throws Exception {
    // Create a MetricsSource directly for testing (no GravitinoEnv needed)
    metricsSource = new IcebergClientMetricsSource();

    // Inject the source into the store via package-private constructor
    store = new IcebergRestMetricsStore(metricsSource);
    Map<String, String> properties = new HashMap<>();
    store.init(properties);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (store != null) {
      store.close();
    }
  }

  @Test
  public void testInitialization() throws Exception {
    IcebergRestMetricsStore testStore = new IcebergRestMetricsStore();
    Assertions.assertDoesNotThrow(() -> testStore.init(new HashMap<>()));
    testStore.close();
  }

  @Test
  public void testRecordCommitReport() throws Exception {
    MetricRegistry registry = metricsSource.getMetricRegistry();
    Assertions.assertNotNull(registry, "MetricRegistry should be available");
    MetricsReport report =
        ImmutableCommitReport.builder()
            .tableName("test_table")
            .snapshotId(1L)
            .sequenceNumber(1L)
            .operation("append")
            .commitMetrics(
                ImmutableCommitMetricsResult.builder()
                    .totalDuration(
                        ImmutableTimerResult.builder()
                            .timeUnit(TimeUnit.NANOSECONDS)
                            .totalDuration(Duration.ofNanos(500000000L)) // 500ms in nanos
                            .count(1)
                            .build())
                    .addedDataFiles(
                        ImmutableCounterResult.builder().unit(Unit.COUNT).value(10L).build())
                    .build())
            .build();

    store.recordMetric(report);

    Histogram totalDurationHistogram = registry.getHistograms().get("iceberg.total-duration");
    Assertions.assertNotNull(totalDurationHistogram, "Histogram should exist");
    Assertions.assertEquals(1, totalDurationHistogram.getCount(), "Histogram should have 1 sample");
    Assertions.assertEquals(
        500.0, totalDurationHistogram.getSnapshot().getMean(), 0.01, "Duration should be 500ms");

    Counter addedFilesCounter = registry.getCounters().get("iceberg.added-data-files");
    Assertions.assertNotNull(addedFilesCounter, "Counter should exist");
    Assertions.assertEquals(10L, addedFilesCounter.getCount(), "Counter should be 10");
  }

  @Test
  public void testMetricsActuallyRecorded() throws Exception {
    MetricRegistry registry = metricsSource.getMetricRegistry();

    MetricsReport report =
        ImmutableCommitReport.builder()
            .tableName("metrics_test_table")
            .snapshotId(123L)
            .sequenceNumber(456L)
            .operation("append")
            .commitMetrics(
                ImmutableCommitMetricsResult.builder()
                    .totalDuration(
                        ImmutableTimerResult.builder()
                            .timeUnit(TimeUnit.MICROSECONDS)
                            .totalDuration(Duration.ofNanos(250000L * 1000)) // 250ms in micros
                            .count(1)
                            .build())
                    .addedDataFiles(
                        ImmutableCounterResult.builder().unit(Unit.COUNT).value(25L).build())
                    .addedRecords(
                        ImmutableCounterResult.builder().unit(Unit.COUNT).value(1000L).build())
                    .build())
            .build();

    store.recordMetric(report);

    Histogram durationHistogram = registry.getHistograms().get("iceberg.total-duration");
    Assertions.assertNotNull(durationHistogram, "total-duration histogram should exist");
    Assertions.assertEquals(1, durationHistogram.getCount(), "Should have 1 duration sample");
    Assertions.assertEquals(250.0, durationHistogram.getSnapshot().getMean(), 0.01);

    Counter filesCounter = registry.getCounters().get("iceberg.added-data-files");
    Assertions.assertNotNull(filesCounter, "added-data-files counter should exist");
    Assertions.assertEquals(25L, filesCounter.getCount(), "Files counter should be 25");

    Counter recordsCounter = registry.getCounters().get("iceberg.added-records");
    Assertions.assertNotNull(recordsCounter, "added-records counter should exist");
    Assertions.assertEquals(1000L, recordsCounter.getCount(), "Records counter should be 1000");

    // Record another report to test accumulation
    MetricsReport report2 =
        ImmutableCommitReport.builder()
            .tableName("metrics_test_table_2")
            .snapshotId(789L)
            .sequenceNumber(101112L)
            .operation("append")
            .commitMetrics(
                ImmutableCommitMetricsResult.builder()
                    .totalDuration(
                        ImmutableTimerResult.builder()
                            .timeUnit(TimeUnit.MILLISECONDS)
                            .totalDuration(Duration.ofMillis(100L)) // 100ms
                            .count(1)
                            .build())
                    .addedDataFiles(
                        ImmutableCounterResult.builder().unit(Unit.COUNT).value(5L).build())
                    .build())
            .build();

    store.recordMetric(report2);

    Assertions.assertEquals(2, durationHistogram.getCount(), "Should have 2 duration samples");
    Assertions.assertEquals(30L, filesCounter.getCount(), "Files counter should be 25 + 5 = 30");
  }

  @Test
  public void testConvertToMillis() {
    Assertions.assertEquals(100L, store.convertToMillis(100000000L, "nanoseconds"));
    Assertions.assertEquals(100L, store.convertToMillis(100000L, "microseconds"));
    Assertions.assertEquals(100L, store.convertToMillis(100L, "milliseconds"));
    Assertions.assertEquals(5000L, store.convertToMillis(5L, "seconds"));

    // Test unknown unit (defaults to nanoseconds)
    Assertions.assertEquals(100L, store.convertToMillis(100000000L, "unknown"));
  }

  @Test
  public void testNullMetricsReport() {
    // Should not throw when given null
    Assertions.assertDoesNotThrow(() -> store.recordMetric(null));
  }

  @Test
  public void testMultipleReports() throws Exception {
    // Record multiple commit reports
    for (int i = 0; i < 5; i++) {
      MetricsReport report =
          ImmutableCommitReport.builder()
              .tableName("test_table_" + i)
              .snapshotId((long) i)
              .sequenceNumber((long) i)
              .operation("append")
              .commitMetrics(ImmutableCommitMetricsResult.builder().build())
              .build();

      Assertions.assertDoesNotThrow(() -> store.recordMetric(report));
    }
  }

  @Test
  public void testCleanOperation() {
    // Clean should not throw (it's a no-op for MetricsSystem lifecycle)
    Assertions.assertDoesNotThrow(() -> store.clean(Instant.now()));
  }

  @Test
  public void testClose() {
    Assertions.assertDoesNotThrow(() -> store.close());

    // Double close should also not throw
    Assertions.assertDoesNotThrow(() -> store.close());
  }

  @Test
  public void testErrorHandling() throws Exception {
    // Create a report with empty metrics (no actual metric data)
    ImmutableCommitMetricsResult emptyMetrics = ImmutableCommitMetricsResult.builder().build();
    MetricsReport report =
        ImmutableCommitReport.builder()
            .tableName("test_table")
            .snapshotId(1L)
            .sequenceNumber(1L)
            .operation("append")
            .commitMetrics(emptyMetrics)
            .build();

    // Should handle reports with no actual metrics gracefully
    Assertions.assertDoesNotThrow(() -> store.recordMetric(report));
  }

  @Test
  public void testConvertToMillisEdgeCases() {
    // Test minutes conversion
    Assertions.assertEquals(60000L, store.convertToMillis(1L, "minutes"));

    // Test hours conversion
    Assertions.assertEquals(3600000L, store.convertToMillis(1L, "hours"));

    // Test days conversion
    Assertions.assertEquals(86400000L, store.convertToMillis(1L, "days"));

    // Test case insensitivity
    Assertions.assertEquals(100L, store.convertToMillis(100000000L, "NANOSECONDS"));
    Assertions.assertEquals(100L, store.convertToMillis(100000L, "MicroSeconds"));
  }
}
