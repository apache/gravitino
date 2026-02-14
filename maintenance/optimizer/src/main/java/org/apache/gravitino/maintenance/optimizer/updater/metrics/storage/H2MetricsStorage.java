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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class H2MetricsStorage implements MetricsRepository {

  private static final Logger LOG = LoggerFactory.getLogger(H2MetricsStorage.class);

  private static String JDBC_URL = "jdbc:h2:file:./metrics_db;AUTO_SERVER=TRUE";
  private static final String USER = "sa";
  private static final String PASSWORD = "";

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  public H2MetricsStorage() {}

  @Override
  public void initialize(Map<String, String> OptimizerProperties) {
    Map<String, String> h2Properties =
        MapUtils.getPrefixMap(
            OptimizerProperties,
            OptimizerConfig.OPTIMIZER_PREFIX + H2MetricsStorageConfig.H2_METRICS_PREFIX);
    H2MetricsStorageConfig config = new H2MetricsStorageConfig(h2Properties);
    String path = config.get(H2MetricsStorageConfig.H2_METRICS_STORAGE_PATH_CONFIG);
    JDBC_URL = "jdbc:h2:file:" + path + ";AUTO_SERVER=TRUE";
    initializeDatabase();
  }

  private void initializeDatabase() {
    String createTableMetricsSql =
        "CREATE TABLE IF NOT EXISTS table_metrics ("
            + "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
            + "table_identifier VARCHAR(1000) NOT NULL, "
            + "metric_name VARCHAR(255) NOT NULL, "
            + "partition VARCHAR(255), "
            + "timestamp BIGINT NOT NULL, "
            + "value VARCHAR(1000) NOT NULL"
            + ")";

    String createJobMetricsSql =
        "CREATE TABLE IF NOT EXISTS job_metrics ("
            + "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
            + "job_identifier VARCHAR(1000) NOT NULL, "
            + "metric_name VARCHAR(255) NOT NULL, "
            + "timestamp BIGINT NOT NULL, "
            + "value VARCHAR(1000) NOT NULL"
            + ")";

    String createIndexSql1 =
        "CREATE INDEX IF NOT EXISTS idx_table_metrics_timestamp ON table_metrics(timestamp)";
    String createIndexSql2 =
        "CREATE INDEX IF NOT EXISTS idx_job_metrics_timestamp ON job_metrics(timestamp)";
    String createIndexSql3 =
        "CREATE INDEX IF NOT EXISTS idx_table_metrics_composite ON table_metrics(table_identifier, partition, timestamp)";

    try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        Statement stmt = conn.createStatement()) {
      stmt.execute(createTableMetricsSql);
      stmt.execute(createJobMetricsSql);
      stmt.execute(createIndexSql1);
      stmt.execute(createIndexSql2);
      stmt.execute(createIndexSql3);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to initialize H2 metrics storage", e);
    }
  }

  @Override
  public void storeTableMetric(
      NameIdentifier nameIdentifier,
      String metricName,
      Optional<String> partition,
      MetricRecord metric) {
    String sql =
        "INSERT INTO table_metrics (table_identifier, metric_name, partition, timestamp, value) VALUES (?, ?, ?, ?, ?)";

    try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, normalizeIdentifier(nameIdentifier));
      pstmt.setString(2, normalizeMetricName(metricName));
      pstmt.setString(3, normalizePartition(partition).orElse(null));
      pstmt.setLong(4, metric.getTimestamp());
      pstmt.setString(5, metric.getValue());
      pstmt.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to store table metric", e);
    }
  }

  @Override
  public Map<String, List<MetricRecord>> getTableMetrics(
      NameIdentifier nameIdentifier, long fromTimestamp, long toTimestamp) {
    Map<String, List<MetricRecord>> resultMap = new HashMap<>();
    StringBuilder sqlBuilder =
        new StringBuilder(
            "SELECT metric_name, timestamp, value FROM table_metrics "
                + "WHERE table_identifier = ? AND timestamp BETWEEN ? AND ?");

    sqlBuilder.append(" AND partition IS NULL ORDER BY timestamp ASC");

    try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        PreparedStatement pstmt = conn.prepareStatement(sqlBuilder.toString())) {
      pstmt.setString(1, normalizeIdentifier(nameIdentifier));
      pstmt.setLong(2, fromTimestamp);
      pstmt.setLong(3, toTimestamp);

      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          String metricName = rs.getString("metric_name");
          long timestamp = rs.getLong("timestamp");
          String value = rs.getString("value");
          MetricRecord metric = new MetricRecordImpl(timestamp, value);
          resultMap.computeIfAbsent(metricName, k -> new ArrayList<>()).add(metric);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to retrieve table metrics", e);
    }
    return resultMap;
  }

  @Override
  public Map<String, List<MetricRecord>> getPartitionMetrics(
      NameIdentifier nameIdentifier, String partition, long fromTimestamp, long toTimestamp) {
    Map<String, List<MetricRecord>> resultMap = new HashMap<>();
    String sql =
        "SELECT metric_name, timestamp, value FROM table_metrics "
            + "WHERE table_identifier = ? AND partition = ? AND timestamp BETWEEN ? AND ? "
            + "ORDER BY timestamp ASC";

    try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, normalizeIdentifier(nameIdentifier));
      pstmt.setString(2, normalizePartition(partition).orElse(null));
      pstmt.setLong(3, fromTimestamp);
      pstmt.setLong(4, toTimestamp);

      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          String metricName = rs.getString("metric_name");
          long timestamp = rs.getLong("timestamp");
          String value = rs.getString("value");
          MetricRecord metric = new MetricRecordImpl(timestamp, value);
          resultMap.computeIfAbsent(metricName, k -> new ArrayList<>()).add(metric);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to retrieve partition metrics", e);
    }
    return resultMap;
  }

  @Override
  public void storeJobMetric(
      NameIdentifier nameIdentifier, String metricName, MetricRecord metric) {
    String sql =
        "INSERT INTO job_metrics (job_identifier, metric_name, timestamp, value) VALUES (?, ?, ?, ?)";

    try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, normalizeIdentifier(nameIdentifier));
      pstmt.setString(2, normalizeMetricName(metricName));
      pstmt.setLong(3, metric.getTimestamp());
      pstmt.setString(4, metric.getValue());
      pstmt.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to store job metric", e);
    }
  }

  @Override
  public Map<String, List<MetricRecord>> getJobMetrics(
      NameIdentifier nameIdentifier, long fromTimestamp, long toTimestamp) {
    Map<String, List<MetricRecord>> resultMap = new HashMap<>();
    String sql =
        "SELECT metric_name, timestamp, value FROM job_metrics "
            + "WHERE job_identifier = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp ASC";

    try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, normalizeIdentifier(nameIdentifier));
      pstmt.setLong(2, fromTimestamp);
      pstmt.setLong(3, toTimestamp);

      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          String metricName = rs.getString("metric_name");
          long timestamp = rs.getLong("timestamp");
          String value = rs.getString("value");
          MetricRecord metric = new MetricRecordImpl(timestamp, value);
          resultMap.computeIfAbsent(metricName, k -> new ArrayList<>()).add(metric);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to retrieve job metrics", e);
    }
    return resultMap;
  }

  private String normalizeIdentifier(NameIdentifier identifier) {
    return identifier == null ? null : identifier.toString().toLowerCase(Locale.ROOT);
  }

  private String normalizeMetricName(String metricName) {
    return metricName == null ? null : metricName.toLowerCase(Locale.ROOT);
  }

  private Optional<String> normalizePartition(String partition) {
    return normalizePartition(Optional.ofNullable(partition));
  }

  private Optional<String> normalizePartition(Optional<String> partition) {
    return partition.map(p -> p.toLowerCase(Locale.ROOT));
  }

  @Override
  public int cleanupTableMetricsBefore(long beforeTimestamp) {
    String sql = "DELETE FROM table_metrics WHERE timestamp < ?";

    try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setLong(1, beforeTimestamp);
      int deletedRows = pstmt.executeUpdate();
      conn.commit();
      LOG.info("Cleaned up {} rows from table_metrics before {}", deletedRows, beforeTimestamp);
      return deletedRows;
    } catch (SQLException e) {
      throw new RuntimeException("Failed to cleanup table metrics", e);
    }
  }

  @Override
  public int cleanupJobMetricsBefore(long beforeTimestamp) {
    String sql = "DELETE FROM job_metrics WHERE timestamp < ?";

    try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setLong(1, beforeTimestamp);
      int deletedRows = pstmt.executeUpdate();
      conn.commit();
      LOG.info("Cleaned up {} rows from job_metrics before {}", deletedRows, beforeTimestamp);
      return deletedRows;
    } catch (SQLException e) {
      throw new RuntimeException("Failed to cleanup job metrics", e);
    }
  }

  public int cleanupAllMetricsBefore(long beforeTimestamp) {
    int totalDeleted =
        cleanupTableMetricsBefore(beforeTimestamp) + cleanupJobMetricsBefore(beforeTimestamp);
    LOG.info("Total cleaned up rows: {}", totalDeleted);
    return totalDeleted;
  }

  @Override
  public void close() {
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public static class H2MetricsStorageConfig extends Config {
    static final String H2_METRICS_PREFIX = "h2-metrics.";

    public static final String H2_METRICS_STORAGE_PATH = "h2-metrics-storage-path";

    public static final ConfigEntry<String> H2_METRICS_STORAGE_PATH_CONFIG =
        new ConfigBuilder(H2_METRICS_STORAGE_PATH)
            .doc("The path for H2 metrics storage.")
            .version(ConfigConstants.VERSION_1_1_0)
            .stringConf()
            .createWithDefault("./data/metrics.db");

    public H2MetricsStorageConfig(Map<String, String> properties) {
      super(false);
      loadFromMap(properties, k -> true);
    }
  }
}
