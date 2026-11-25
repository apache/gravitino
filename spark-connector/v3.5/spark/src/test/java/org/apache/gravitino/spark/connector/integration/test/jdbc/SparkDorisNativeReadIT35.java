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

package org.apache.gravitino.spark.connector.integration.test.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.apache.doris.spark.read.DorisInputPartition;
import org.apache.doris.spark.read.DorisScanV2;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.integration.test.container.DorisContainer;
import org.apache.gravitino.spark.connector.jdbc.doris.GravitinoDorisCatalogSpark35;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.execution.datasources.v2.V1ScanWrapper;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import scala.collection.Iterator;

/** Integration tests for native Doris reads and JDBC query routing. */
@Tag("gravitino-docker-test")
@Tag("doris-multi-version")
public class SparkDorisNativeReadIT35 extends DorisNativeReadFixture35 {
  @Test
  void testNativePartitionsProjectionAndPredicate() {
    Dataset<Row> data =
        getSparkSession().sql("SELECT id FROM " + sparkTable() + " WHERE id >= 3 AND id <= 6");
    List<Integer> ids =
        data.collectAsList().stream()
            .map(row -> row.getInt(0))
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(List.of(3, 4, 5, 6), ids);

    Scan scan = getScan(data);
    Assertions.assertInstanceOf(DorisScanV2.class, scan);
    Assertions.assertArrayEquals(new String[] {"id"}, scan.readSchema().fieldNames());
    InputPartition[] partitions = scan.toBatch().planInputPartitions();
    Assertions.assertTrue(
        partitions.length > 1, "native scan should plan multiple tablet partitions");
    for (InputPartition partition : partitions) {
      DorisInputPartition doris = Assertions.assertInstanceOf(DorisInputPartition.class, partition);
      Assertions.assertEquals(List.of("`id`"), Arrays.asList(doris.readCols()));
      Assertions.assertTrue(doris.predicates().length > 0, "predicate must reach the Doris reader");
    }
  }

  @Test
  void testLimitUsesJdbcAndReturnsCorrectRows() {
    Dataset<Row> data =
        getSparkSession().sql("SELECT id FROM " + sparkTable() + " ORDER BY id LIMIT 3");
    Assertions.assertEquals(
        List.of(1, 2, 3),
        data.collectAsList().stream().map(row -> row.getInt(0)).collect(Collectors.toList()));
    Assertions.assertInstanceOf(JDBCScan.class, getScan(data));
  }

  @Test
  void testTimestampReadsUseJdbcWhenConnectionTimezoneDiffers() throws Exception {
    String table = "native_timestamps";
    try (Connection connection =
            DriverManager.getConnection(
                jdbcUrl, DorisContainer.USER_NAME, DorisContainer.PASSWORD);
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE "
              + databaseName
              + "."
              + table
              + " (id INT NOT NULL, event_time DATETIME) DISTRIBUTED BY HASH(id) BUCKETS 2");
      statement.execute(
          "INSERT INTO "
              + databaseName
              + "."
              + table
              + " VALUES (1, '2026-01-01 00:00:00'), (2, '2026-01-02 00:00:00'),"
              + " (3, '2026-01-01 16:00:00')");
    }
    TimeZone previous = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
      Assertions.assertEquals("Asia/Shanghai", TimeZone.getDefault().getID());
      Map<String, String> nativeProperties = new HashMap<>(getCatalogConfigs());
      nativeProperties.put("spark.bypass.connectionTimeZone", "UTC");
      nativeProperties.put("spark.bypass.preserveInstants", "true");
      Map<String, String> jdbcProperties = new HashMap<>(nativeProperties);
      jdbcProperties.keySet().removeIf(key -> key.startsWith("spark.bypass.doris."));
      jdbcProperties.put("spark.bypass.pushDownPredicate", "false");
      String nativeCatalog = "jdbc_doris_native_tz";
      String jdbcCatalog = "jdbc_doris_baseline_tz";
      client
          .loadMetalake("test")
          .createCatalog(
              nativeCatalog, Catalog.Type.RELATIONAL, "jdbc-doris", "", nativeProperties);
      client
          .loadMetalake("test")
          .createCatalog(jdbcCatalog, Catalog.Type.RELATIONAL, "jdbc-doris", "", jdbcProperties);
      for (String catalog : List.of(nativeCatalog, jdbcCatalog)) {
        getSparkSession()
            .conf()
            .set("spark.sql.catalog." + catalog, GravitinoDorisCatalogSpark35.class.getName());
      }

      String nativeTable = nativeCatalog + "." + databaseName + "." + table;
      String jdbcTable = jdbcCatalog + "." + databaseName + "." + table;
      Instant expected = Instant.parse("2026-01-01T00:00:00Z");
      for (String source : List.of(nativeTable, jdbcTable)) {
        Dataset<Row> timestamp =
            getSparkSession().sql("SELECT event_time FROM " + source + " WHERE id = 1");
        Assertions.assertEquals(
            expected, timestamp.collectAsList().get(0).getTimestamp(0).toInstant());
        Assertions.assertInstanceOf(JDBCScan.class, getScan(timestamp));

        Dataset<Row> filtered =
            getSparkSession()
                .sql(
                    "SELECT id FROM "
                        + source
                        + " WHERE event_time > TIMESTAMP '2026-01-01 12:00:00'");
        Assertions.assertEquals(
            List.of(2, 3),
            filtered.collectAsList().stream()
                .map(row -> row.getInt(0))
                .sorted()
                .collect(Collectors.toList()));
        Assertions.assertInstanceOf(JDBCScan.class, getScan(filtered));
      }

      Dataset<Row> ordinary =
          getSparkSession().sql("SELECT id FROM " + nativeTable + " WHERE id = 1");
      Assertions.assertEquals(1, ordinary.collectAsList().get(0).getInt(0));
      Assertions.assertInstanceOf(DorisScanV2.class, getScan(ordinary));
    } finally {
      TimeZone.setDefault(previous);
    }
  }

  static Scan getScan(Dataset<Row> data) {
    Iterator<LogicalPlan> leaves = data.queryExecution().optimizedPlan().collectLeaves().iterator();
    while (leaves.hasNext()) {
      LogicalPlan leaf = leaves.next();
      if (leaf instanceof DataSourceV2ScanRelation) {
        Scan scan = ((DataSourceV2ScanRelation) leaf).scan();
        return scan instanceof V1ScanWrapper ? ((V1ScanWrapper) scan).v1Scan() : scan;
      }
    }
    throw new AssertionError("No data source scan was planned");
  }
}
