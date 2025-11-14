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

import static org.apache.gravitino.iceberg.service.metrics.JDBCMetricsStore.getCounterResult;
import static org.apache.gravitino.iceberg.service.metrics.JDBCMetricsStore.getTimerResult;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.json.JsonUtils;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.CommitMetrics;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class TestJdbcMetricsStore {

  private static final String CURRENT_SCRIPT_VERSION = "1.1.0";
  private static final Map<String, Map<String, String>> dbProperties = Maps.newHashMap();

  @BeforeAll
  public static void beforeAll() {
    ContainerSuite containerSuite = ContainerSuite.getInstance();
    containerSuite.startMySQLContainer(TestDatabaseName.MYSQL_JDBC_BACKEND);
    containerSuite.startPostgreSQLContainer(TestDatabaseName.PG_JDBC_BACKEND);
    // Prepare test configurations
    Map<String, String> h2Properties = Maps.newHashMap();
    h2Properties.put("jdbc-metrics.jdbc-driver", "org.h2.Driver");
    h2Properties.put("jdbc-metrics.uri", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=MYSQL");
    dbProperties.put("h2", h2Properties);

    Map<String, String> mysqlProperties = Maps.newHashMap();
    mysqlProperties.put("jdbc-metrics.jdbc-driver", "com.mysql.cj.jdbc.Driver");
    mysqlProperties.put(
        "jdbc-metrics.uri",
        containerSuite.getMySQLContainer().getJdbcUrl(TestDatabaseName.MYSQL_JDBC_BACKEND));
    mysqlProperties.put("jdbc-metrics.jdbc-user", containerSuite.getMySQLContainer().getUsername());
    mysqlProperties.put(
        "jdbc-metrics.jdbc-password", containerSuite.getMySQLContainer().getPassword());
    dbProperties.put("mysql", mysqlProperties);
    Map<String, String> postgresqlProperties = Maps.newHashMap();
    postgresqlProperties.put("jdbc-metrics.jdbc-driver", "org.postgresql.Driver");
    postgresqlProperties.put(
        "jdbc-metrics.uri", containerSuite.getPostgreSQLContainer().getJdbcUrl());
    postgresqlProperties.put(
        "jdbc-metrics.jdbc-user", containerSuite.getPostgreSQLContainer().getUsername());
    postgresqlProperties.put(
        "jdbc-metrics.jdbc-password", containerSuite.getPostgreSQLContainer().getPassword());
    dbProperties.put("postgresql", postgresqlProperties);
  }

  @Test
  public void testJdbcMetricsStore() throws Exception {
    CommitMetrics commitMetrics = CommitMetrics.of(new DefaultMetricsContext());
    commitMetrics.totalDuration().record(100, TimeUnit.SECONDS);
    commitMetrics.attempts().increment(4);
    Map<String, String> snapshotSummary =
        ImmutableMap.<String, String>builder()
            .put(SnapshotSummary.ADDED_FILES_PROP, "1")
            .put(SnapshotSummary.DELETED_FILES_PROP, "2")
            .put(SnapshotSummary.TOTAL_DATA_FILES_PROP, "3")
            .put(SnapshotSummary.ADDED_DELETE_FILES_PROP, "4")
            .put(SnapshotSummary.ADD_EQ_DELETE_FILES_PROP, "5")
            .put(SnapshotSummary.ADD_POS_DELETE_FILES_PROP, "6")
            .put(SnapshotSummary.REMOVED_POS_DELETE_FILES_PROP, "7")
            .put(SnapshotSummary.REMOVED_EQ_DELETE_FILES_PROP, "8")
            .put(SnapshotSummary.REMOVED_DELETE_FILES_PROP, "9")
            .put(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "10")
            .put(SnapshotSummary.ADDED_RECORDS_PROP, "11")
            .put(SnapshotSummary.DELETED_RECORDS_PROP, "12")
            .put(SnapshotSummary.TOTAL_RECORDS_PROP, "13")
            .put(SnapshotSummary.ADDED_FILE_SIZE_PROP, "14")
            .put(SnapshotSummary.REMOVED_FILE_SIZE_PROP, "15")
            .put(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "16")
            .put(SnapshotSummary.ADDED_POS_DELETES_PROP, "17")
            .put(SnapshotSummary.ADDED_EQ_DELETES_PROP, "18")
            .put(SnapshotSummary.REMOVED_POS_DELETES_PROP, "19")
            .put(SnapshotSummary.REMOVED_EQ_DELETES_PROP, "20")
            .put(SnapshotSummary.TOTAL_POS_DELETES_PROP, "21")
            .put(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "22")
            .build();

    String tableName = "tableName";
    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName(tableName)
            .snapshotId(23L)
            .operation("DELETE")
            .sequenceNumber(4L)
            .commitMetrics(CommitMetricsResult.from(commitMetrics, snapshotSummary))
            .build();

    ScanMetrics scanMetrics = ScanMetrics.of(new DefaultMetricsContext());
    scanMetrics.totalPlanningDuration().record(10, TimeUnit.MINUTES);
    scanMetrics.resultDataFiles().increment(5L);
    scanMetrics.resultDeleteFiles().increment(5L);
    scanMetrics.scannedDataManifests().increment(5L);
    scanMetrics.totalFileSizeInBytes().increment(1024L);
    scanMetrics.totalDataManifests().increment(5L);

    int schemaId = 4;
    List<Integer> fieldIds = Arrays.asList(1, 2);
    List<String> fieldNames = Arrays.asList("c1", "c2");

    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName(tableName)
            .snapshotId(23L)
            .filter(Expressions.alwaysTrue())
            .schemaId(schemaId)
            .projectedFieldIds(fieldIds)
            .projectedFieldNames(fieldNames)
            .scanMetrics(ScanMetricsResult.fromScanMetrics(scanMetrics))
            .build();

    for (Map.Entry<String, Map<String, String>> entry : dbProperties.entrySet()) {
      JDBCMetricsStore metricsStore = new JDBCMetricsStore();
      metricsStore.initProperties(entry.getValue());

      String gravitinoHome = System.getenv("GRAVITINO_ROOT_DIR");
      String mysqlContent =
          FileUtils.readFileToString(
              new File(
                  gravitinoHome
                      + String.format(
                          "/scripts/%s/iceberg-metrics-schema-%s-%s.sql",
                          entry.getKey(), CURRENT_SCRIPT_VERSION, entry.getKey())),
              "UTF-8");

      String[] sqls =
          Arrays.stream(mysqlContent.split(";"))
              .map(String::trim)
              .filter(s -> !s.isEmpty())
              .toArray(String[]::new);
      // Init the store to create tables
      for (String sql : sqls) {
        metricsStore.execute(sql);
      }

      metricsStore.recordMetric("a", Namespace.of("a"), commitReport);
      metricsStore.recordMetric("a", Namespace.of("b"), scanReport);

      String countSql = "SELECT COUNT(*) AS total FROM commit_metrics_report";
      Integer count = metricsStore.connections.run(getTotal(countSql));
      String selectCommitReportSql = "SELECT * FROM commit_metrics_report";
      metricsStore.connections.run(validateCommitReport(selectCommitReportSql, commitReport));

      Assertions.assertEquals(1, count);
      String countSql2 = "SELECT COUNT(*) AS total FROM scan_metrics_report";
      count = metricsStore.connections.run(getTotal(countSql2));
      Assertions.assertEquals(1, count);
      String selectScanReportSql = "SELECT * FROM scan_metrics_report";
      metricsStore.connections.run(validateScanReport(selectScanReportSql, scanReport));

      metricsStore.clean(Instant.now());

      count = metricsStore.connections.run(getTotal(countSql));
      Assertions.assertEquals(0, count);
      count = metricsStore.connections.run(getTotal(countSql2));
      Assertions.assertEquals(0, count);

      metricsStore.close();
    }
  }

  private static ClientPool.Action<Integer, Connection, SQLException> getTotal(String countSql2) {
    return conn -> {
      try (var stmt = conn.createStatement();
          var rs = stmt.executeQuery(countSql2)) {
        if (rs.next()) {
          return rs.getInt("total");
        }
        return 0;
      }
    };
  }

  private static ClientPool.Action<Void, Connection, SQLException> validateCommitReport(
      String sql, CommitReport commitReport) {
    return conn -> {
      try (var stmt = conn.createStatement()) {
        var rs = stmt.executeQuery(sql);
        if (rs.next()) {
          Assertions.assertEquals("a.a", rs.getString("namespace"));
          Assertions.assertEquals(commitReport.tableName(), rs.getString("table_name"));
          Assertions.assertEquals(commitReport.snapshotId(), rs.getLong("snapshot_id"));
          Assertions.assertEquals(commitReport.sequenceNumber(), rs.getLong("sequence_number"));
          Assertions.assertEquals(commitReport.operation(), rs.getString("operation"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().addedDataFiles()),
              rs.getLong("added_data_files"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().removedDataFiles()),
              rs.getLong("removed_data_files"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().totalDataFiles()),
              rs.getLong("total_data_files"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().addedDeleteFiles()),
              rs.getLong("added_delete_files"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().addedEqualityDeleteFiles()),
              rs.getLong("added_equality_delete_files"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().addedPositionalDeleteFiles()),
              rs.getLong("added_positional_delete_files"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().removedDeleteFiles()),
              rs.getLong("removed_delete_files"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().removedEqualityDeleteFiles()),
              rs.getLong("removed_equality_delete_files"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().removedPositionalDeleteFiles()),
              rs.getLong("removed_positional_delete_files"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().totalDeleteFiles()),
              rs.getLong("total_delete_files"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().addedRecords()),
              rs.getLong("added_records"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().removedRecords()),
              rs.getLong("removed_records"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().totalRecords()),
              rs.getLong("total_records"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().addedFilesSizeInBytes()),
              rs.getLong("added_files_size_in_bytes"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().removedFilesSizeInBytes()),
              rs.getLong("removed_files_size_in_bytes"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().totalFilesSizeInBytes()),
              rs.getLong("total_files_size_in_bytes"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().addedPositionalDeletes()),
              rs.getLong("added_positional_deletes"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().removedPositionalDeletes()),
              rs.getLong("removed_positional_deletes"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().totalPositionalDeletes()),
              rs.getLong("total_positional_deletes"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().addedEqualityDeleteFiles()),
              rs.getLong("added_equality_deletes"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().removedEqualityDeleteFiles()),
              rs.getLong("removed_equality_deletes"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().totalEqualityDeletes()),
              rs.getLong("total_equality_deletes"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().manifestsCreated()),
              rs.getLong("manifests_created"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().manifestsReplaced()),
              rs.getLong("manifests_replaced"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().manifestsKept()),
              rs.getLong("manifests_kept"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().manifestEntriesProcessed()),
              rs.getLong("manifest_entries_processed"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().addedDVs()), rs.getLong("added_dvs"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().removedDVs()),
              rs.getLong("removed_dvs"));
          Assertions.assertEquals(
              getTimerResult(commitReport.commitMetrics().totalDuration()),
              rs.getLong("total_duration_ms"));
          Assertions.assertEquals(
              getCounterResult(commitReport.commitMetrics().attempts()), rs.getLong("attempts"));
          Assertions.assertEquals(
              JsonUtils.objectMapper().writeValueAsString(commitReport.metadata()),
              rs.getString("metadata"));

        } else {
          Assertions.fail("No data found in commit_metrics_report");
        }
      } catch (Exception e) {
        Assertions.fail("Exception occurred while validating commit report: " + e.getMessage());
      }
      return null;
    };
  }

  private static ClientPool.Action<Void, Connection, SQLException> validateScanReport(
      String sql, ScanReport scanReport) {
    return conn -> {
      try (var stmt = conn.createStatement()) {
        var rs = stmt.executeQuery(sql);
        if (rs.next()) {
          Assertions.assertEquals(scanReport.tableName(), rs.getString("table_name"));
          Assertions.assertEquals(scanReport.snapshotId(), rs.getLong("snapshot_id"));
          Assertions.assertEquals(scanReport.schemaId(), rs.getInt("schema_id"));

          Assertions.assertEquals(scanReport.filter().toString(), rs.getString("filter"));
          Assertions.assertEquals(
              JsonUtils.objectMapper().writeValueAsString(scanReport.metadata()),
              rs.getString("metadata"));
          Assertions.assertEquals(
              JsonUtils.objectMapper().writeValueAsString(scanReport.projectedFieldIds()),
              rs.getString("projected_field_ids"));
          Assertions.assertEquals(
              JsonUtils.objectMapper()
                  .writeValueAsString(scanReport.projectedFieldNames().toString()),
              rs.getString("projected_field_names"));

          Assertions.assertEquals(
              getCounterResult(scanReport.scanMetrics().equalityDeleteFiles()),
              rs.getLong("equality_delete_files"));
          Assertions.assertEquals(
              getCounterResult(scanReport.scanMetrics().indexedDeleteFiles()),
              rs.getLong("indexed_delete_files"));
          Assertions.assertEquals(
              getCounterResult(scanReport.scanMetrics().positionalDeleteFiles()),
              rs.getLong("positional_delete_files"));
          Assertions.assertEquals(
              getCounterResult(scanReport.scanMetrics().resultDataFiles()),
              rs.getLong("result_data_files"));
          Assertions.assertEquals(
              getCounterResult(scanReport.scanMetrics().resultDeleteFiles()),
              rs.getLong("result_delete_files"));
          Assertions.assertEquals(
              getCounterResult(scanReport.scanMetrics().scannedDataManifests()),
              rs.getLong("scanned_data_manifests"));
          Assertions.assertEquals(
              getCounterResult(scanReport.scanMetrics().scannedDeleteManifests()),
              rs.getLong("scanned_delete_manifests"));
          Assertions.assertEquals(
              getCounterResult(scanReport.scanMetrics().skippedDataFiles()),
              rs.getLong("skipped_data_files"));
          Assertions.assertEquals(
              getCounterResult(scanReport.scanMetrics().skippedDataManifests()),
              rs.getLong("skipped_data_manifests"));
          Assertions.assertEquals(
              getCounterResult(scanReport.scanMetrics().skippedDeleteFiles()),
              rs.getLong("skipped_delete_files"));
          Assertions.assertEquals(
              getCounterResult(scanReport.scanMetrics().skippedDeleteManifests()),
              rs.getLong("skipped_delete_manifests"));
          Assertions.assertEquals(
              getCounterResult(scanReport.scanMetrics().totalDataManifests()),
              rs.getLong("total_data_manifests"));
          Assertions.assertEquals(
              getCounterResult(scanReport.scanMetrics().totalDeleteFileSizeInBytes()),
              rs.getLong("total_delete_file_size_in_bytes"));
          Assertions.assertEquals(
              getCounterResult(scanReport.scanMetrics().totalDeleteManifests()),
              rs.getLong("total_delete_manifests"));
          Assertions.assertEquals(
              getCounterResult(scanReport.scanMetrics().totalFileSizeInBytes()),
              rs.getLong("total_file_size_in_bytes"));
          Assertions.assertEquals(
              getTimerResult(scanReport.scanMetrics().totalPlanningDuration()),
              rs.getLong("total_planning_duration"));

        } else {
          Assertions.fail("No data found in scan_metrics_report");
        }
      } catch (Exception e) {
        Assertions.fail("Exception occurred while validating scan report: " + e.getMessage());
      }
      return null;
    };
  }
}
