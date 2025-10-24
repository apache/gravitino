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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.IcebergClientMetricsSource;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CommitReportParser;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.ScanReportParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg metrics store that exposes client metrics through Gravitino's MetricsSystem.
 *
 * <p>This store extracts numeric metrics from Iceberg {@link MetricsReport} instances (commit and
 * scan reports) and records them to Gravitino's metrics system as counters and histograms. The
 * metrics are then exposed via standard endpoints:
 *
 * <ul>
 *   <li>{@code /metrics} - JSON format
 *   <li>{@code /prometheus/metrics} - Prometheus format
 * </ul>
 *
 * <p>The implementation uses generic metric extraction based on JSON structure pattern matching:
 *
 * <ul>
 *   <li>Fields with "total-duration" → TimerResult → Recorded to Histogram (in milliseconds)
 *   <li>Fields with "value" → CounterResult → Recorded to Counter
 * </ul>
 *
 * <p>This approach is future-proof as it handles any metric names without hardcoding, automatically
 * supporting new metrics added by Iceberg.
 *
 * <p>Configuration:
 *
 * <pre>
 * gravitino.iceberg-rest.metricsStore = rest
 * </pre>
 */
public class IcebergRestMetricsStore implements IcebergMetricsStore {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergRestMetricsStore.class);
  public static final String ICEBERG_METRICS_STORE_NAME = "rest";

  private IcebergClientMetricsSource metricsSource;
  private MetricsSystem metricsSystem;
  private final ObjectMapper objectMapper = new ObjectMapper();

  /** Default constructor for production use. */
  public IcebergRestMetricsStore() {}

  /**
   * Package-private constructor for testing. Allows injection of a MetricsSource without requiring
   * GravitinoEnv to be initialized.
   *
   * @param metricsSource the metrics source to use for recording metrics
   */
  @VisibleForTesting
  IcebergRestMetricsStore(IcebergClientMetricsSource metricsSource) {
    this.metricsSource = metricsSource;
  }

  @Override
  public void init(Map<String, String> properties) throws IOException {
    try {
      // Create and register the metrics source (if not already provided via constructor)
      if (this.metricsSource == null) {
        this.metricsSource = new IcebergClientMetricsSource();
      }

      // Get the MetricsSystem instance from GravitinoEnv (only if not in test mode)
      if (this.metricsSystem == null) {
        this.metricsSystem = GravitinoEnv.getInstance().metricsSystem();
      }

      // Register the source with the metrics system if available
      // In test environments, metricsSystem may be null
      if (this.metricsSystem != null) {
        this.metricsSystem.register(metricsSource);
        LOG.info(
            "Initialized GravitinoMetricsStore - Iceberg client metrics will be exposed via /metrics and /prometheus/metrics");
      } else {
        LOG.warn(
            "MetricsSystem not available - metrics will be recorded to source but not registered with system (test mode)");
      }
    } catch (Exception e) {
      LOG.error("Failed to initialize GravitinoMetricsStore", e);
      throw new IOException("Failed to initialize GravitinoMetricsStore", e);
    }
  }

  @Override
  public void recordMetric(MetricsReport metricsReport) throws IOException {
    if (metricsReport == null) {
      return;
    }

    try {
      // Track report types
      if (metricsReport instanceof CommitReport) {
        metricsSource.getCounter("iceberg.reports.commit").inc();
      } else if (metricsReport instanceof ScanReport) {
        metricsSource.getCounter("iceberg.reports.scan").inc();
      }

      // Extract and record all metrics
      extractAndRecordMetrics(metricsReport);

    } catch (Exception e) {
      LOG.warn("Failed to record Iceberg metrics: {}", metricsReport, e);
      metricsSource.getCounter("iceberg.metrics.extraction.errors").inc();
      // Don't throw - we don't want to break the metrics pipeline
    }
  }

  /**
   * Extracts metrics from a report and records them in the MetricsSystem.
   *
   * <p>Handles both CommitReport and ScanReport by accessing the metrics directly from the Java
   * objects, then converting to JSON for generic field traversal.
   *
   * @param report the metrics report to extract from
   */
  @VisibleForTesting
  void extractAndRecordMetrics(MetricsReport report) {
    try {
      String reportJson = null;

      // Serialize the entire report using Iceberg's public parsers
      if (report instanceof CommitReport) {
        reportJson = CommitReportParser.toJson((CommitReport) report);
      } else if (report instanceof ScanReport) {
        reportJson = ScanReportParser.toJson((ScanReport) report);
      }

      if (reportJson != null) {
        JsonNode reportNode = objectMapper.readTree(reportJson);
        // Extract the "metrics" field from the report JSON
        JsonNode metricsNode = reportNode.get("metrics");
        if (metricsNode != null && metricsNode.isObject()) {
          // Walk all metric fields generically
          metricsNode
              .fields()
              .forEachRemaining(
                  entry -> {
                    String metricName = entry.getKey();
                    JsonNode metricValue = entry.getValue();
                    // Pattern match by structure (not by name)
                    recordMetricValue(metricName, metricValue);
                  });
        }
      }

    } catch (Exception e) {
      LOG.warn("Failed to extract metrics from report: {}", report, e);
      metricsSource.getCounter("iceberg.metrics.extraction.errors").inc();
    }
  }
  /**
   * Records a single metric value by pattern matching its JSON structure.
   *
   * <p>Iceberg guarantees only two metric value types:
   *
   * <ul>
   *   <li>TimerResult: {"time-unit": "...", "count": 1, "total-duration": 5000}
   *   <li>CounterResult: {"unit": "...", "value": 123}
   * </ul>
   *
   * @param metricName the metric name (e.g., "total-duration", "added-data-files")
   * @param metricValue the metric value JSON node
   */
  @VisibleForTesting
  void recordMetricValue(String metricName, JsonNode metricValue) {
    LOG.debug("Recording metric: {} = {}", metricName, metricValue);
    try {
      // Pattern 1: TimerResult (has "total-duration" field)
      if (metricValue.has("total-duration")) {
        long totalDurationNanos = metricValue.get("total-duration").asLong();
        String timeUnit =
            metricValue.has("time-unit") ? metricValue.get("time-unit").asText() : "nanoseconds";

        long durationMillis = convertToMillis(totalDurationNanos, timeUnit);

        // Record to histogram (distribution of durations)
        metricsSource.getHistogram("iceberg." + metricName).update(durationMillis);

        // Also track count if present (multiple samples in one report)
        if (metricValue.has("count")) {
          long count = metricValue.get("count").asLong();
          if (count > 1) {
            metricsSource.getCounter("iceberg." + metricName + ".count").inc(count);
          }
        }
      }
      // Pattern 2: CounterResult (has "value" field)
      else if (metricValue.has("value")) {
        long value = metricValue.get("value").asLong();
        metricsSource.getCounter("iceberg." + metricName).inc(value);
      }
      // Unknown structure (defensive logging)
      else {
        LOG.debug("Unknown metric structure for '{}': {}", metricName, metricValue);
        metricsSource.getCounter("iceberg.metrics.unknown").inc();
      }
    } catch (Exception e) {
      LOG.warn("Failed to record metric '{}': {}", metricName, metricValue, e);
      metricsSource.getCounter("iceberg.metrics.recording.errors").inc();
    }
  }

  /**
   * Converts duration from Iceberg's time unit to milliseconds.
   *
   * @param duration the duration value
   * @param timeUnitStr the time unit string (e.g., "nanoseconds", "milliseconds")
   * @return duration in milliseconds
   */
  @VisibleForTesting
  long convertToMillis(long duration, String timeUnitStr) {
    TimeUnit timeUnit;
    switch (timeUnitStr.toLowerCase()) {
      case "nanoseconds":
        timeUnit = TimeUnit.NANOSECONDS;
        break;
      case "microseconds":
        timeUnit = TimeUnit.MICROSECONDS;
        break;
      case "milliseconds":
        timeUnit = TimeUnit.MILLISECONDS;
        break;
      case "seconds":
        timeUnit = TimeUnit.SECONDS;
        break;
      case "minutes":
        timeUnit = TimeUnit.MINUTES;
        break;
      case "hours":
        timeUnit = TimeUnit.HOURS;
        break;
      case "days":
        timeUnit = TimeUnit.DAYS;
        break;
      default:
        LOG.warn("Unknown time unit '{}', treating as nanoseconds", timeUnitStr);
        timeUnit = TimeUnit.NANOSECONDS;
    }

    return timeUnit.toMillis(duration);
  }

  @Override
  public synchronized void clean(Instant expireTime) throws IOException {
    // Gravitino's MetricsSystem handles metric lifecycle
    // Sliding time windows automatically clean old data
    // No action needed here
  }

  /**
   * Returns the metrics source for testing purposes.
   *
   * @return the IcebergClientMetricsSource instance, or null if not initialized
   */
  @VisibleForTesting
  IcebergClientMetricsSource getMetricsSource() {
    return metricsSource;
  }

  @Override
  public void close() throws IOException {
    try {
      if (metricsSystem != null && metricsSource != null) {
        metricsSystem.unregister(metricsSource);
        LOG.info("Closed GravitinoMetricsStore and unregistered metrics source");
      }
    } catch (Exception e) {
      LOG.error("Failed to close GravitinoMetricsStore", e);
      throw new IOException("Failed to close GravitinoMetricsStore", e);
    }
  }
}
