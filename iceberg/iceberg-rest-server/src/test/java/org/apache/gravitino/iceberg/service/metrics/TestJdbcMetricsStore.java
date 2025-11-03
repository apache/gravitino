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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class TestJdbcMetricsStore {

  private static final String CURRENT_SCRIPT_VERSION = "1.1.0";
  ContainerSuite containerSuite = ContainerSuite.getInstance();

  @Test
  public void testJdbcMetricsStore() throws Exception {
    // Start container
    containerSuite.startMySQLContainer(TestDatabaseName.MYSQL_JDBC_BACKEND);
    containerSuite.startPostgreSQLContainer(TestDatabaseName.PG_JDBC_BACKEND);

    // Prepare test configurations
    Map<String, Map<String, String>> dbProperties = Maps.newHashMap();
    Map<String, String> h2Properties = Maps.newHashMap();
    h2Properties.put("jdbc-driver", "org.h2.Driver");
    h2Properties.put("uri", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=MYSQL");
    dbProperties.put("h2", h2Properties);

    Map<String, String> mysqlProperties = Maps.newHashMap();
    mysqlProperties.put("jdbc-driver", "com.mysql.cj.jdbc.Driver");
    mysqlProperties.put(
        "uri", containerSuite.getMySQLContainer().getJdbcUrl(TestDatabaseName.MYSQL_JDBC_BACKEND));
    mysqlProperties.put("jdbc-user", containerSuite.getMySQLContainer().getUsername());
    mysqlProperties.put("jdbc-password", containerSuite.getMySQLContainer().getPassword());
    dbProperties.put("mysql", mysqlProperties);
    Map<String, String> postgresqlProperties = Maps.newHashMap();
    postgresqlProperties.put("jdbc-driver", "org.postgresql.Driver");
    postgresqlProperties.put("uri", containerSuite.getPostgreSQLContainer().getJdbcUrl());
    postgresqlProperties.put("jdbc-user", containerSuite.getPostgreSQLContainer().getUsername());
    postgresqlProperties.put(
        "jdbc-password", containerSuite.getPostgreSQLContainer().getPassword());
    dbProperties.put("postgresql", postgresqlProperties);

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
      metricsStore.init(entry.getValue());

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

      metricsStore.recordMetric(Namespace.of("a"), commitReport);
      metricsStore.recordMetric(Namespace.of("b"), scanReport);

      String countSql = "SELECT COUNT(*) AS total FROM commit_metrics_report";
      Integer count = metricsStore.connections.run(getTotal(countSql));
      Assertions.assertEquals(1, count);
      String countSql2 = "SELECT COUNT(*) AS total FROM scan_metrics_report";
      count = metricsStore.connections.run(getTotal(countSql2));
      Assertions.assertEquals(1, count);

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
}
