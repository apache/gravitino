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

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.doris.spark.read.DorisScanV2;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.integration.test.container.DorisContainer;
import org.apache.gravitino.spark.connector.jdbc.doris.GravitinoDorisCatalogSpark35;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Exercises native Doris reads with real remote Spark executors. Select this suite explicitly with
 * {@code -PdorisStandaloneIT -PdorisMultiVersionTest} and configure its standalone Spark settings.
 */
@Tag("gravitino-docker-test")
@Tag("doris-multi-version")
@Tag("doris-standalone")
public class SparkDorisNativeDistributedIT35 extends DorisNativeReadFixture35 {
  @Override
  protected String getSparkMaster() {
    String master = System.getenv("GRAVITINO_TEST_SPARK_MASTER");
    if (master == null || !master.startsWith("spark://")) {
      throw new IllegalStateException(
          "GRAVITINO_TEST_SPARK_MASTER must name a standalone Spark master");
    }
    return master;
  }

  @Override
  protected Map<String, String> getExtraSparkConfigs() {
    Map<String, String> configs = new HashMap<>(super.getExtraSparkConfigs());
    String jars = System.getenv("GRAVITINO_TEST_SPARK_JARS");
    String driverHost = System.getenv("GRAVITINO_TEST_SPARK_DRIVER_HOST");
    if (jars == null || jars.isEmpty() || driverHost == null || driverHost.isEmpty()) {
      throw new IllegalStateException(
          "The distributed test requires Spark JARs and a reachable driver host");
    }
    configs.put("spark.jars", jars);
    configs.put("spark.driver.host", driverHost);
    configs.put("spark.executor.instances", "2");
    configs.put("spark.executor.cores", "1");
    configs.put("spark.cores.max", "2");
    return configs;
  }

  @Test
  void testNativePartitionsReadOnTwoExecutors() throws Exception {
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .until(
            () -> getSparkSession().sparkContext().statusTracker().getExecutorInfos().length >= 3);
    Dataset<Row> data = getSparkSession().sql("SELECT id FROM " + sparkTable());
    Scan scan = SparkDorisNativeReadIT35.getScan(data);
    Assertions.assertInstanceOf(DorisScanV2.class, scan);
    assertReaderMode(scan);
    Set<String> executors = ConcurrentHashMap.newKeySet();
    SparkListener listener =
        new SparkListener() {
          @Override
          public void onTaskEnd(SparkListenerTaskEnd event) {
            if (event.taskInfo().successful()) {
              executors.add(event.taskInfo().executorId());
            }
          }
        };
    getSparkSession().sparkContext().addSparkListener(listener);
    try {
      List<Row> rows = data.collectAsList();
      Set<Integer> values = new HashSet<>();
      for (Row row : rows) {
        values.add(row.getInt(0));
      }
      Assertions.assertEquals(Set.of(1, 2, 3, 4, 5, 6, 7, 8), values);
      Assertions.assertEquals(8, rows.size());
      Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> executors.size() >= 2);
      Assertions.assertFalse(executors.contains("driver"));
    } finally {
      getSparkSession().sparkContext().removeSparkListener(listener);
    }

    String jdbcCatalog = "jdbc_doris_compare";
    Map<String, String> properties = new HashMap<>(getCatalogConfigs());
    properties.keySet().removeIf(key -> key.startsWith("spark.bypass.doris."));
    client
        .loadMetalake("test")
        .createCatalog(jdbcCatalog, Catalog.Type.RELATIONAL, "jdbc-doris", "", properties);
    getSparkSession()
        .conf()
        .set("spark.sql.catalog." + jdbcCatalog, GravitinoDorisCatalogSpark35.class.getName());

    String query = "SELECT id, name FROM %s WHERE id >= 3";
    Dataset<Row> nativeRows = getSparkSession().sql(String.format(query, sparkTable()));
    Dataset<Row> jdbcRows =
        getSparkSession()
            .sql(String.format(query, jdbcCatalog + "." + databaseName + "." + TABLE_NAME));
    Scan nativeScan = SparkDorisNativeReadIT35.getScan(nativeRows);
    Scan jdbcScan = SparkDorisNativeReadIT35.getScan(jdbcRows);
    Assertions.assertInstanceOf(DorisScanV2.class, nativeScan);
    assertReaderMode(nativeScan);
    Assertions.assertInstanceOf(JDBCScan.class, jdbcScan);
    Assertions.assertTrue(nativeScan.toBatch().planInputPartitions().length > 1);
    List<Row> nativeResult = nativeRows.collectAsList();
    List<Row> jdbcResult = jdbcRows.collectAsList();
    Assertions.assertEquals(6, nativeResult.size());
    Assertions.assertEquals(jdbcResult.size(), nativeResult.size());
    Assertions.assertEquals(new HashSet<>(jdbcResult), new HashSet<>(nativeResult));
  }

  @Test
  void testNativeReadQuotesReservedTableName() throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                jdbcUrl, DorisContainer.USER_NAME, DorisContainer.PASSWORD);
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE "
              + databaseName
              + ".`select`"
              + " (id INT NOT NULL) DISTRIBUTED BY HASH(id) BUCKETS 2");
      statement.execute("INSERT INTO " + databaseName + ".`select` VALUES (1), (2)");
    }
    Dataset<Row> data =
        getSparkSession().sql("SELECT id FROM " + CATALOG_NAME + "." + databaseName + ".`select`");
    Scan scan = SparkDorisNativeReadIT35.getScan(data);
    Assertions.assertInstanceOf(DorisScanV2.class, scan);
    assertReaderMode(scan);
    Set<Integer> values = new HashSet<>();
    for (Row row : data.collectAsList()) {
      values.add(row.getInt(0));
    }
    Assertions.assertEquals(Set.of(1, 2), values);
  }

  private void assertReaderMode(Scan scan) throws ReflectiveOperationException {
    // The official 26.1.0 reader does not expose its selected mode through the Spark Scan API.
    Object factory = scan.toBatch().createReaderFactory();
    Field mode = factory.getClass().getDeclaredField("mode");
    mode.setAccessible(true);
    Assertions.assertEquals(readMode().toUpperCase(Locale.ROOT), mode.get(factory).toString());
  }
}
