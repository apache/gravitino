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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.utils.MapUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.jdbc.UncheckedInterruptedException;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.TimerResult;

public class JDBCMetricsStore implements IcebergMetricsStore {
  public static final String ICEBERG_METRICS_STORE_JDBC_NAME = "jdbc";
  private static final String URI = "uri";
  private static final String INSERT_COMMIT_REPORT_METRICS_SQL =
      "INSERT INTO commit_metrics_report ("
          + "timestamp, namespace, table_name, snapshot_id, sequence_number, operation,"
          + "added_data_files, removed_data_files, total_data_files,"
          + "added_delete_files, added_equality_delete_files,  added_positional_delete_files, "
          + "removed_delete_files, removed_equality_delete_files, removed_positional_delete_files, total_delete_files,"
          + "added_records, removed_records, total_records,"
          + "added_files_size_in_bytes, removed_files_size_in_bytes, total_files_size_in_bytes,"
          + "added_positional_deletes, removed_positional_deletes, total_positional_deletes,"
          + "added_equality_deletes, removed_equality_deletes, total_equality_deletes,"
          + "manifests_created, manifests_replaced, manifests_kept, manifest_entries_processed,"
          + "added_dvs, removed_dvs,"
          + "total_duration_ms, attempts, metadata"
          + ") VALUES "
          + "(?, ?, ?, ?, ?, ?,"
          + " ?, ?, ?,"
          + " ?, ?, ?,"
          + " ?, ?, ?, ?,"
          + " ?, ?, ?,"
          + " ?, ?, ?,"
          + " ?, ?, ?,"
          + " ?, ?, ?,"
          + " ?, ?, ?, ?,"
          + " ?, ?,"
          + " ?, ?, ?);";

  private static final String INSERT_SCAN_REPORT_METRICS_SQL =
      "INSERT INTO scan_metrics_report ("
          + "timestamp, namespace, table_name, snapshot_id, schema_id, "
          + "filter, metadata, projected_field_ids, projected_field_names, "
          + "equality_delete_files, indexed_delete_files, positional_delete_files, "
          + "result_data_files, result_delete_files, "
          + "scanned_data_manifests, scanned_delete_manifests, "
          + "skipped_data_files, skipped_data_manifests, "
          + "skipped_delete_files, skipped_delete_manifests, "
          + "total_data_manifests, total_delete_file_size_in_bytes, "
          + "total_delete_manifests, total_file_size_in_bytes,"
          + "total_planning_duration) VALUES "
          + "(?, ?, ?, ?, ?,"
          + "?, ?, ?, ?,"
          + "?, ? ,?,"
          + "?, ?,"
          + "?, ?,"
          + "?, ?,"
          + "?, ?,"
          + "?, ?,"
          + "?, ?,"
          + "?);";

  private static final String DELETE_EXPIRED_SCAN_METRICS_SQL =
      "DELETE FROM scan_metrics_report WHERE timestamp < ?; ";

  private static final String DELETE_EXPIRED_COMMIT_METRICS_SQL =
      "DELETE FROM commit_metrics_report WHERE timestamp < ?;";

  @VisibleForTesting JdbcClientPool connections;

  @Override
  public void init(Map<String, String> properties) throws IOException {
    initProperties(properties);
    checkMetricsReportTableExists();
  }

  private void checkMetricsReportTableExists() {
    try {
      Preconditions.checkArgument(
          connections.run(
              conn -> {
                DatabaseMetaData dbMeta = conn.getMetaData();
                ResultSet commitReportTableExists =
                    dbMeta.getTables(
                        null /* catalog name */,
                        null /* schemaPattern */,
                        "commit_metrics_report" /* tableNamePattern */,
                        null /* types */);
                ResultSet scanReportTableExists =
                    dbMeta.getTables(
                        null /* catalog name */,
                        null /* schemaPattern */,
                        "scan_metrics_report" /* tableNamePattern */,
                        null /* types */);

                return commitReportTableExists.next() && scanReportTableExists.next();
              }),
          "JDBC metrics store tables do not exist. You should use the sql scripts under directory `scripts` to initialize the database");
    } catch (SQLException | InterruptedException exception) {
      throw new RuntimeException(exception);
    }
  }

  @VisibleForTesting
  void initProperties(Map<String, String> properties) {
    Map<String, String> actualProps = MapUtils.getPrefixMap(properties, "jdbc-metrics.");
    String uri = actualProps.get(URI);
    Preconditions.checkArgument(uri != null, "JDBC metrics store requires a \"%s\" property", URI);

    connections =
        new JdbcClientPool(uri, IcebergPropertiesUtils.toIcebergCatalogProperties(actualProps));
  }

  @Override
  public void recordMetric(String catalog, Namespace namespace, MetricsReport metricsReport)
      throws IOException {
    if (metricsReport instanceof CommitReport) {
      CommitReport commitReport = (CommitReport) metricsReport;
      execute(
          INSERT_COMMIT_REPORT_METRICS_SQL,
          Instant.now().toEpochMilli(),
          String.format("%s.%s", catalog, namespace.toString()),
          commitReport.tableName(),
          commitReport.snapshotId(),
          commitReport.sequenceNumber(),
          commitReport.operation(),
          getCounterResult(commitReport.commitMetrics().addedDataFiles()),
          getCounterResult(commitReport.commitMetrics().removedDataFiles()),
          getCounterResult(commitReport.commitMetrics().totalDataFiles()),
          getCounterResult(commitReport.commitMetrics().addedDeleteFiles()),
          getCounterResult(commitReport.commitMetrics().addedEqualityDeleteFiles()),
          getCounterResult(commitReport.commitMetrics().addedPositionalDeleteFiles()),
          getCounterResult(commitReport.commitMetrics().removedDeleteFiles()),
          getCounterResult(commitReport.commitMetrics().removedEqualityDeleteFiles()),
          getCounterResult(commitReport.commitMetrics().removedPositionalDeleteFiles()),
          getCounterResult(commitReport.commitMetrics().totalDeleteFiles()),
          getCounterResult(commitReport.commitMetrics().addedRecords()),
          getCounterResult(commitReport.commitMetrics().removedRecords()),
          getCounterResult(commitReport.commitMetrics().totalRecords()),
          getCounterResult(commitReport.commitMetrics().addedFilesSizeInBytes()),
          getCounterResult(commitReport.commitMetrics().removedFilesSizeInBytes()),
          getCounterResult(commitReport.commitMetrics().totalFilesSizeInBytes()),
          getCounterResult(commitReport.commitMetrics().addedPositionalDeletes()),
          getCounterResult(commitReport.commitMetrics().removedPositionalDeletes()),
          getCounterResult(commitReport.commitMetrics().totalPositionalDeletes()),
          getCounterResult(commitReport.commitMetrics().addedEqualityDeleteFiles()),
          getCounterResult(commitReport.commitMetrics().removedEqualityDeleteFiles()),
          getCounterResult(commitReport.commitMetrics().totalEqualityDeletes()),
          getCounterResult(commitReport.commitMetrics().manifestsCreated()),
          getCounterResult(commitReport.commitMetrics().manifestsReplaced()),
          getCounterResult(commitReport.commitMetrics().manifestsKept()),
          getCounterResult(commitReport.commitMetrics().manifestEntriesProcessed()),
          getCounterResult(commitReport.commitMetrics().addedDVs()),
          getCounterResult(commitReport.commitMetrics().removedDVs()),
          getTimerResult(commitReport.commitMetrics().totalDuration()),
          getCounterResult(commitReport.commitMetrics().attempts()),
          JsonUtils.objectMapper().writeValueAsString(commitReport.metadata()));
    } else if (metricsReport instanceof ScanReport) {
      ScanReport scanReport = (ScanReport) metricsReport;
      execute(
          INSERT_SCAN_REPORT_METRICS_SQL,
          Instant.now().toEpochMilli(),
          String.format("%s.%s", catalog, namespace.toString()),
          scanReport.tableName(),
          scanReport.snapshotId(),
          scanReport.schemaId(),
          scanReport.filter().toString(),
          JsonUtils.objectMapper().writeValueAsString(scanReport.metadata()),
          JsonUtils.objectMapper().writeValueAsString(scanReport.projectedFieldIds()),
          JsonUtils.objectMapper().writeValueAsString(scanReport.projectedFieldNames().toString()),
          getCounterResult(scanReport.scanMetrics().equalityDeleteFiles()),
          getCounterResult(scanReport.scanMetrics().indexedDeleteFiles()),
          getCounterResult(scanReport.scanMetrics().positionalDeleteFiles()),
          getCounterResult(scanReport.scanMetrics().resultDataFiles()),
          getCounterResult(scanReport.scanMetrics().resultDeleteFiles()),
          getCounterResult(scanReport.scanMetrics().scannedDataManifests()),
          getCounterResult(scanReport.scanMetrics().scannedDeleteManifests()),
          getCounterResult(scanReport.scanMetrics().skippedDataFiles()),
          getCounterResult(scanReport.scanMetrics().skippedDataManifests()),
          getCounterResult(scanReport.scanMetrics().skippedDeleteFiles()),
          getCounterResult(scanReport.scanMetrics().skippedDeleteManifests()),
          getCounterResult(scanReport.scanMetrics().totalDataManifests()),
          getCounterResult(scanReport.scanMetrics().totalDeleteFileSizeInBytes()),
          getCounterResult(scanReport.scanMetrics().totalDeleteManifests()),
          getCounterResult(scanReport.scanMetrics().totalFileSizeInBytes()),
          getTimerResult(scanReport.scanMetrics().totalPlanningDuration()));
    }
  }

  @Override
  public void clean(Instant expireTime) throws IOException {
    execute(DELETE_EXPIRED_SCAN_METRICS_SQL, expireTime.toEpochMilli());
    execute(DELETE_EXPIRED_COMMIT_METRICS_SQL, expireTime.toEpochMilli());
  }

  @Override
  public void close() throws IOException {
    if (connections != null) {
      connections.close();
    }
  }

  @VisibleForTesting
  static long getCounterResult(CounterResult result) {
    return result != null ? result.value() : 0L;
  }

  @VisibleForTesting
  static long getTimerResult(TimerResult result) {
    return result != null ? result.count() : 0L;
  }

  @VisibleForTesting
  int execute(String sql, Object... args) {
    return execute(err -> {}, sql, args);
  }

  private int execute(Consumer<SQLException> sqlErrorHandler, String sql, Object... args) {
    try {
      return connections.run(
          conn -> {
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
              for (int pos = 0; pos < args.length; pos += 1) {
                if (args[pos] instanceof Long) {
                  preparedStatement.setLong(pos + 1, (Long) args[pos]);
                } else if (args[pos] instanceof String) {
                  preparedStatement.setString(pos + 1, (String) args[pos]);
                } else if (args[pos] instanceof Integer) {
                  preparedStatement.setInt(pos + 1, (Integer) args[pos]);
                } else {
                  throw new IllegalArgumentException(
                      "Unsupported argument type: " + args[pos].getClass());
                }
              }

              return preparedStatement.executeUpdate();
            }
          });
    } catch (SQLException e) {
      sqlErrorHandler.accept(e);
      throw new UncheckedSQLException(e, "Failed to execute: %s", sql);
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Interrupted in SQL command");
    }
  }
}
