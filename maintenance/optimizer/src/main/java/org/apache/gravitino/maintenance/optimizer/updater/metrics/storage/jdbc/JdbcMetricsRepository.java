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

package org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.JobMetricWriteRequest;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricRecord;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricRecordImpl;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricsRepository;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricsStorageException;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.TableMetricWriteRequest;
import org.apache.gravitino.utils.jdbc.JdbcSqlScriptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC-based implementation of {@link MetricsRepository}.
 *
 * <p>All timestamps are epoch seconds. Read APIs use a half-open time window [fromSecs, toSecs).
 */
public abstract class JdbcMetricsRepository implements MetricsRepository {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcMetricsRepository.class);
  private static final int BATCH_UPDATE_SIZE = 1000;
  private static final int MAX_METRIC_VALUE_LENGTH = 1024;
  private static final int MAX_IDENTIFIER_LENGTH = 1024;
  private static final int MAX_METRIC_NAME_LENGTH = 1024;
  private static final long MAX_REASONABLE_EPOCH_SECONDS = 9_999_999_999L;
  private static final String DATABASE_PRODUCT_H2 = "h2";
  private static final String H2_SCHEMA_FILE_NAME =
      "optimizer-metrics-schema-" + ConfigConstants.CURRENT_SCRIPT_VERSION + "-h2.sql";
  private static final String TABLE_METRICS_TABLE = "table_metrics";
  private static final String JOB_METRICS_TABLE = "job_metrics";
  private static final Set<String> REQUIRED_TABLE_METRICS_COLUMNS =
      Set.of("table_identifier", "metric_name", "table_partition", "metric_ts", "metric_value");
  private static final Set<String> REQUIRED_JOB_METRICS_COLUMNS =
      Set.of("job_identifier", "metric_name", "metric_ts", "metric_value");

  protected static final String DEFAULT_USER = "sa";
  protected static final String DEFAULT_PASSWORD = "";

  private DataSourceJdbcConnectionProvider connectionProvider;
  private JdbcMetricsDialect dialect;
  private volatile boolean initialized = false;

  protected final void initializeStorage(DataSourceJdbcConnectionProvider connectionProvider) {
    Preconditions.checkState(!initialized, "JdbcMetricsRepository has already been initialized.");
    Preconditions.checkArgument(connectionProvider != null, "connectionProvider must not be null");

    this.connectionProvider = connectionProvider;
    initializeDatabase();
    this.initialized = true;
  }

  private void initializeDatabase() {
    try (Connection conn = connectionProvider.getConnection()) {
      this.dialect = detectDialect(conn);
      maybeInitializeSchema(conn);
      validateSchema(conn);
    } catch (SQLException e) {
      String dialectName = dialect == null ? "unknown" : dialect.name();
      throw new MetricsStorageException(
          "Failed to initialize JDBC metrics storage, dialect=" + dialectName, e);
    }
  }

  private void validateSchema(Connection connection) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    checkRequiredTable(metaData, TABLE_METRICS_TABLE);
    checkRequiredTable(metaData, JOB_METRICS_TABLE);
    checkRequiredColumns(metaData, TABLE_METRICS_TABLE, REQUIRED_TABLE_METRICS_COLUMNS);
    checkRequiredColumns(metaData, JOB_METRICS_TABLE, REQUIRED_JOB_METRICS_COLUMNS);
  }

  private void checkRequiredTable(DatabaseMetaData metaData, String tableName) throws SQLException {
    if (hasTable(metaData, tableName)) {
      return;
    }

    throw new MetricsStorageException(
        "Required table '"
            + tableName
            + "' is missing. Initialize metrics schema with SQL scripts before startup.",
        new IllegalStateException("Missing table: " + tableName));
  }

  private void checkRequiredColumns(
      DatabaseMetaData metaData, String tableName, Set<String> requiredColumns)
      throws SQLException {
    Set<String> existingColumns = listColumns(metaData, tableName);
    Set<String> missingColumns = new HashSet<>(requiredColumns);
    missingColumns.removeAll(existingColumns);
    if (!missingColumns.isEmpty()) {
      throw new MetricsStorageException(
          "Table '"
              + tableName
              + "' is missing required columns "
              + missingColumns
              + ". Run schema migration before startup.",
          new IllegalStateException(
              "Missing required columns in " + tableName + ": " + missingColumns));
    }
  }

  private boolean hasTable(DatabaseMetaData metaData, String tableName) throws SQLException {
    try (ResultSet rs = metaData.getTables(null, null, tableName, new String[] {"TABLE"})) {
      if (rs.next()) {
        return true;
      }
    }
    try (ResultSet rs =
        metaData.getTables(
            null, null, tableName.toUpperCase(Locale.ROOT), new String[] {"TABLE"})) {
      if (rs.next()) {
        return true;
      }
    }
    try (ResultSet rs =
        metaData.getTables(
            null, null, tableName.toLowerCase(Locale.ROOT), new String[] {"TABLE"})) {
      return rs.next();
    }
  }

  private Set<String> listColumns(DatabaseMetaData metaData, String tableName) throws SQLException {
    Set<String> columns = new HashSet<>();
    collectColumns(metaData, tableName, columns);
    collectColumns(metaData, tableName.toUpperCase(Locale.ROOT), columns);
    collectColumns(metaData, tableName.toLowerCase(Locale.ROOT), columns);
    return columns;
  }

  private void collectColumns(DatabaseMetaData metaData, String tableName, Set<String> columns)
      throws SQLException {
    try (ResultSet rs = metaData.getColumns(null, null, tableName, null)) {
      while (rs.next()) {
        String columnName = rs.getString("COLUMN_NAME");
        if (StringUtils.isNotBlank(columnName)) {
          columns.add(columnName.toLowerCase(Locale.ROOT));
        }
      }
    }
  }

  private JdbcMetricsDialect detectDialect(Connection connection) throws SQLException {
    String databaseProduct =
        connection.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
    if (databaseProduct.contains(DATABASE_PRODUCT_H2)) {
      return new H2MetricsDialect();
    }

    throw new IllegalArgumentException(
        "Unsupported JDBC database product for metrics repository: " + databaseProduct);
  }

  private void maybeInitializeSchema(Connection connection) {
    if (!(dialect instanceof H2MetricsDialect)) {
      return;
    }

    try {
      executeH2SchemaSql(connection);
    } catch (IOException | SQLException | IllegalStateException e) {
      throw new MetricsStorageException(
          "Failed to initialize H2 metrics schema from script: " + H2_SCHEMA_FILE_NAME, e);
    }
  }

  private void executeH2SchemaSql(Connection connection) throws IOException, SQLException {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Preconditions.checkState(
        StringUtils.isNotBlank(gravitinoHome),
        "GRAVITINO_HOME is not set. Cannot locate H2 metrics schema script.");
    Path scriptPath = Paths.get(gravitinoHome, "scripts", "h2", H2_SCHEMA_FILE_NAME);
    Preconditions.checkState(
        Files.exists(scriptPath),
        "H2 metrics schema script not found at %s. Please ensure distribution scripts are present.",
        scriptPath);
    executeSchemaSql(connection, Files.readString(scriptPath, StandardCharsets.UTF_8));
  }

  private void executeSchemaSql(Connection connection, String sqlContent) throws SQLException {
    JdbcSqlScriptUtils.executeSqlScript(connection, sqlContent);
  }

  @Override
  public void storeTableMetrics(List<TableMetricWriteRequest> metrics) {
    Preconditions.checkArgument(metrics != null, "metrics must not be null");
    if (metrics.isEmpty()) {
      return;
    }

    String insertSql =
        "INSERT INTO table_metrics (table_identifier, metric_name, table_partition, metric_ts, metric_value) VALUES (?, ?, ?, ?, ?)";

    try (Connection conn = getConnection();
        PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
      int batchCount = 0;
      for (TableMetricWriteRequest request : metrics) {
        Preconditions.checkArgument(request != null, "table metric request must not be null");
        validateWriteArguments(
            request.nameIdentifier(), request.metricName(), request.partition(), request.metric());
        String normalizedIdentifier = normalizeIdentifier(request.nameIdentifier());
        String normalizedMetricName = normalizeMetricName(request.metricName());
        Optional<String> normalizedPartition = normalizePartition(request.partition());
        MetricRecord metric = request.metric();
        insertStmt.setString(1, normalizedIdentifier);
        insertStmt.setString(2, normalizedMetricName);
        insertStmt.setString(3, normalizedPartition.orElse(null));
        insertStmt.setLong(4, metric.getTimestamp());
        insertStmt.setString(5, metric.getValue());
        insertStmt.addBatch();
        batchCount++;
        if (batchCount >= BATCH_UPDATE_SIZE) {
          insertStmt.executeBatch();
          batchCount = 0;
        }
      }
      if (batchCount > 0) {
        insertStmt.executeBatch();
      }
    } catch (SQLException e) {
      throw new MetricsStorageException(
          "Failed to batch store table metrics, size=" + metrics.size(), e);
    }
  }

  @Override
  public Map<String, List<MetricRecord>> getTableMetrics(
      NameIdentifier nameIdentifier, long fromSecs, long toSecs) {
    Preconditions.checkArgument(nameIdentifier != null, "nameIdentifier must not be null");
    validateTimeWindow(fromSecs, toSecs);
    Map<String, List<MetricRecord>> resultMap = new HashMap<>();
    String sql =
        "SELECT metric_name, metric_ts, metric_value FROM table_metrics "
            + "WHERE table_identifier = ? AND metric_ts >= ? AND metric_ts < ? "
            + "AND table_partition IS NULL";

    try (Connection conn = getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, normalizeIdentifier(nameIdentifier));
      pstmt.setLong(2, fromSecs);
      pstmt.setLong(3, toSecs);

      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          String metricName = rs.getString("metric_name");
          long timestamp = rs.getLong("metric_ts");
          String value = rs.getString("metric_value");
          MetricRecord metric = new MetricRecordImpl(timestamp, value);
          resultMap.computeIfAbsent(metricName, k -> new ArrayList<>()).add(metric);
        }
      }
    } catch (SQLException e) {
      throw new MetricsStorageException(
          "Failed to retrieve table metrics: identifier="
              + nameIdentifier
              + ", from="
              + fromSecs
              + ", to="
              + toSecs,
          e);
    }
    return resultMap;
  }

  @Override
  public Map<String, List<MetricRecord>> getPartitionMetrics(
      NameIdentifier nameIdentifier, String partition, long fromSecs, long toSecs) {
    Preconditions.checkArgument(nameIdentifier != null, "nameIdentifier must not be null");
    Preconditions.checkArgument(StringUtils.isNotBlank(partition), "partition must not be blank");
    validateTimeWindow(fromSecs, toSecs);
    Map<String, List<MetricRecord>> resultMap = new HashMap<>();
    String normalizedPartition = normalizePartition(partition).orElse(null);
    String sql =
        "SELECT metric_name, metric_ts, metric_value FROM table_metrics "
            + "WHERE table_identifier = ? AND table_partition = ? "
            + "AND metric_ts >= ? AND metric_ts < ?";

    try (Connection conn = getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, normalizeIdentifier(nameIdentifier));
      pstmt.setString(2, normalizedPartition);
      pstmt.setLong(3, fromSecs);
      pstmt.setLong(4, toSecs);

      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          String metricName = rs.getString("metric_name");
          long timestamp = rs.getLong("metric_ts");
          String value = rs.getString("metric_value");
          MetricRecord metric = new MetricRecordImpl(timestamp, value);
          resultMap.computeIfAbsent(metricName, k -> new ArrayList<>()).add(metric);
        }
      }
    } catch (SQLException e) {
      throw new MetricsStorageException(
          "Failed to retrieve partition metrics: identifier="
              + nameIdentifier
              + ", partition="
              + partition
              + ", from="
              + fromSecs
              + ", to="
              + toSecs,
          e);
    }
    return resultMap;
  }

  @Override
  public void storeJobMetrics(List<JobMetricWriteRequest> metrics) {
    Preconditions.checkArgument(metrics != null, "metrics must not be null");
    if (metrics.isEmpty()) {
      return;
    }

    String insertSql =
        "INSERT INTO job_metrics (job_identifier, metric_name, metric_ts, metric_value) VALUES (?, ?, ?, ?)";

    try (Connection conn = getConnection();
        PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
      int batchCount = 0;
      for (JobMetricWriteRequest request : metrics) {
        Preconditions.checkArgument(request != null, "job metric request must not be null");
        validateWriteArguments(
            request.nameIdentifier(), request.metricName(), Optional.empty(), request.metric());
        String normalizedIdentifier = normalizeIdentifier(request.nameIdentifier());
        String normalizedMetricName = normalizeMetricName(request.metricName());
        MetricRecord metric = request.metric();
        insertStmt.setString(1, normalizedIdentifier);
        insertStmt.setString(2, normalizedMetricName);
        insertStmt.setLong(3, metric.getTimestamp());
        insertStmt.setString(4, metric.getValue());
        insertStmt.addBatch();
        batchCount++;
        if (batchCount >= BATCH_UPDATE_SIZE) {
          insertStmt.executeBatch();
          batchCount = 0;
        }
      }
      if (batchCount > 0) {
        insertStmt.executeBatch();
      }
    } catch (SQLException e) {
      throw new MetricsStorageException(
          "Failed to batch store job metrics, size=" + metrics.size(), e);
    }
  }

  @Override
  public Map<String, List<MetricRecord>> getJobMetrics(
      NameIdentifier nameIdentifier, long fromSecs, long toSecs) {
    Preconditions.checkArgument(nameIdentifier != null, "nameIdentifier must not be null");
    validateTimeWindow(fromSecs, toSecs);
    Map<String, List<MetricRecord>> resultMap = new HashMap<>();
    String sql =
        "SELECT metric_name, metric_ts, metric_value FROM job_metrics "
            + "WHERE job_identifier = ? AND metric_ts >= ? AND metric_ts < ?";

    try (Connection conn = getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, normalizeIdentifier(nameIdentifier));
      pstmt.setLong(2, fromSecs);
      pstmt.setLong(3, toSecs);

      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          String metricName = rs.getString("metric_name");
          long timestamp = rs.getLong("metric_ts");
          String value = rs.getString("metric_value");
          MetricRecord metric = new MetricRecordImpl(timestamp, value);
          resultMap.computeIfAbsent(metricName, k -> new ArrayList<>()).add(metric);
        }
      }
    } catch (SQLException e) {
      throw new MetricsStorageException(
          "Failed to retrieve job metrics: identifier="
              + nameIdentifier
              + ", from="
              + fromSecs
              + ", to="
              + toSecs,
          e);
    }
    return resultMap;
  }

  @Override
  public int cleanupTableMetricsBefore(long beforeTimestamp) {
    Preconditions.checkArgument(
        beforeTimestamp >= 0, "beforeTimestamp must be non-negative, but got %s", beforeTimestamp);
    Preconditions.checkArgument(
        beforeTimestamp <= MAX_REASONABLE_EPOCH_SECONDS,
        "beforeTimestamp must be epoch seconds, but got suspiciously large value %s",
        beforeTimestamp);
    String sql = "DELETE FROM table_metrics WHERE metric_ts < ?";

    try (Connection conn = getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setLong(1, beforeTimestamp);
      int deletedRows = pstmt.executeUpdate();
      LOG.info("Cleaned up {} rows from table_metrics before {}", deletedRows, beforeTimestamp);
      return deletedRows;
    } catch (SQLException e) {
      throw new MetricsStorageException(
          "Failed to cleanup table metrics before timestamp: " + beforeTimestamp, e);
    }
  }

  @Override
  public int cleanupJobMetricsBefore(long beforeTimestamp) {
    Preconditions.checkArgument(
        beforeTimestamp >= 0, "beforeTimestamp must be non-negative, but got %s", beforeTimestamp);
    Preconditions.checkArgument(
        beforeTimestamp <= MAX_REASONABLE_EPOCH_SECONDS,
        "beforeTimestamp must be epoch seconds, but got suspiciously large value %s",
        beforeTimestamp);
    String sql = "DELETE FROM job_metrics WHERE metric_ts < ?";

    try (Connection conn = getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setLong(1, beforeTimestamp);
      int deletedRows = pstmt.executeUpdate();
      LOG.info("Cleaned up {} rows from job_metrics before {}", deletedRows, beforeTimestamp);
      return deletedRows;
    } catch (SQLException e) {
      throw new MetricsStorageException(
          "Failed to cleanup job metrics before timestamp: " + beforeTimestamp, e);
    }
  }

  private Connection getConnection() throws SQLException {
    ensureInitialized();
    return connectionProvider.getConnection();
  }

  private void ensureInitialized() {
    Preconditions.checkState(
        initialized,
        "JdbcMetricsRepository has not been initialized. Call initialize(properties) before use.");
  }

  @Override
  public void close() {
    if (connectionProvider != null) {
      connectionProvider.close();
    }
  }

  private void validateTimeWindow(long fromSecs, long toSecs) {
    Preconditions.checkArgument(
        fromSecs < toSecs,
        "Invalid time window: fromSecs (%s) must be less than toSecs (%s)",
        fromSecs,
        toSecs);
  }

  private void validateWriteArguments(
      NameIdentifier nameIdentifier,
      String metricName,
      Optional<String> partition,
      MetricRecord metric) {
    Preconditions.checkArgument(nameIdentifier != null, "nameIdentifier must not be null");
    Preconditions.checkArgument(StringUtils.isNotBlank(metricName), "metricName must not be blank");
    Preconditions.checkArgument(
        nameIdentifier.toString().length() <= MAX_IDENTIFIER_LENGTH,
        "nameIdentifier length exceeds max %s: actual=%s",
        MAX_IDENTIFIER_LENGTH,
        nameIdentifier.toString().length());
    Preconditions.checkArgument(
        metricName.length() <= MAX_METRIC_NAME_LENGTH,
        "metricName length exceeds max %s: actual=%s",
        MAX_METRIC_NAME_LENGTH,
        metricName.length());
    Preconditions.checkArgument(metric != null, "metric record must not be null");
    Preconditions.checkArgument(
        metric.getTimestamp() >= 0,
        "metric timestamp must be non-negative, but got %s",
        metric.getTimestamp());
    Preconditions.checkArgument(
        metric.getTimestamp() <= MAX_REASONABLE_EPOCH_SECONDS,
        "metric timestamp must be epoch seconds, but got suspiciously large value %s",
        metric.getTimestamp());
    Preconditions.checkArgument(metric.getValue() != null, "metric value must not be null");
    Preconditions.checkArgument(
        metric.getValue().length() <= MAX_METRIC_VALUE_LENGTH,
        "metric value length exceeds max %s: actual=%s",
        MAX_METRIC_VALUE_LENGTH,
        metric.getValue().length());
    if (partition.isPresent()) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(partition.get()), "partition must not be blank");
    }
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
}
