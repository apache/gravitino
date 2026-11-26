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

import static org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER;
import static org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD;
import static org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConstants.GRAVITINO_JDBC_URL;
import static org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConstants.GRAVITINO_JDBC_USER;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.doris.spark.read.DorisInputPartition;
import org.apache.doris.spark.read.DorisScanV2;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.DorisContainer;
import org.apache.gravitino.integration.test.container.DorisImageName;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.integration.test.SparkEnvIT;
import org.apache.gravitino.spark.connector.jdbc.doris.GravitinoDorisCatalogSpark35;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.execution.datasources.v2.V1ScanWrapper;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import scala.collection.Iterator;

/** Integration tests for native Doris reads and JDBC query routing. */
@Tag("gravitino-docker-test")
@Tag("doris-multi-version")
public class SparkDorisNativeReadIT35 extends SparkEnvIT {
  private static final String CATALOG_NAME = "jdbc_doris";
  private static final String TABLE_NAME = "select";
  private final String databaseName = suiteDatabaseName("spark_native");
  private String jdbcUrl;
  private String feHttpAddress;
  private int feFlightPort;

  @Override
  protected String getCatalogName() {
    return CATALOG_NAME;
  }

  @Override
  protected String getProvider() {
    return "jdbc-doris";
  }

  @Override
  protected boolean supportsFunction() {
    return false;
  }

  @Override
  protected Map<String, String> getExtraSparkConfigs() {
    Map<String, String> configs = new HashMap<>();
    configs.put(GravitinoSparkConfig.GRAVITINO_ENABLE_DORIS_SUPPORT, "true");
    if (standalone()) {
      String master = System.getenv("GRAVITINO_TEST_SPARK_MASTER");
      String jars = System.getenv("GRAVITINO_TEST_SPARK_JARS");
      String driverHost = System.getenv("GRAVITINO_TEST_SPARK_DRIVER_HOST");
      if (master == null || !master.startsWith("spark://")) {
        throw new IllegalStateException(
            "GRAVITINO_TEST_SPARK_MASTER must name a standalone Spark master");
      }
      if (jars == null || jars.isEmpty() || driverHost == null || driverHost.isEmpty()) {
        throw new IllegalStateException(
            "The distributed test requires Spark JARs and a reachable driver host");
      }
      configs.put("spark.master", master);
      configs.put("spark.jars", jars);
      configs.put("spark.driver.host", driverHost);
      configs.put("spark.executor.instances", "2");
      configs.put("spark.executor.cores", "1");
      configs.put("spark.cores.max", "2");
    }
    return configs;
  }

  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> properties = new HashMap<>();
    properties.put(GRAVITINO_JDBC_URL, jdbcUrl);
    properties.put(GRAVITINO_JDBC_USER, DorisContainer.USER_NAME);
    properties.put(GRAVITINO_JDBC_PASSWORD, DorisContainer.PASSWORD);
    properties.put(GRAVITINO_JDBC_DRIVER, "com.mysql.cj.jdbc.Driver");
    properties.put("credential-providers", JdbcCredential.JDBC_CREDENTIAL_TYPE);
    properties.put("spark.bypass.doris.fenodes", feHttpAddress);
    properties.put("spark.bypass.doris.read.mode", readMode());
    if ("arrow".equalsIgnoreCase(readMode())) {
      properties.put(
          "spark.bypass.doris.read.arrow-flight-sql.port", Integer.toString(feFlightPort));
    }
    properties.put("spark.bypass.doris.request.tablet.size", "1");
    return properties;
  }

  @Override
  protected void initCatalogEnv() throws Exception {
    ContainerSuite suite = ContainerSuite.getInstance();
    suite.startDorisContainer(DorisImageName.VERSION_3_0);
    DorisContainer container = suite.getDorisContainer(DorisImageName.VERSION_3_0);
    jdbcUrl =
        String.format(
            "jdbc:mysql://%s:%d/", container.getContainerIpAddress(), container.getFeMysqlPort());
    try (Connection connection =
            DriverManager.getConnection(
                jdbcUrl, DorisContainer.USER_NAME, DorisContainer.PASSWORD);
        Statement statement = connection.createStatement()) {
      try (ResultSet frontends = statement.executeQuery("SHOW FRONTENDS")) {
        if (!frontends.next()) {
          throw new IllegalStateException("Doris has no frontend");
        }
        feHttpAddress = frontends.getString("Host") + ":" + frontends.getInt("HttpPort");
        if ("arrow".equalsIgnoreCase(readMode())) {
          feFlightPort = frontends.getInt("ArrowFlightSqlPort");
          if (feFlightPort <= 0) {
            throw new IllegalStateException("Doris FE Arrow Flight SQL is not enabled");
          }
        }
      }
      statement.execute("CREATE DATABASE " + databaseName);
      statement.execute(
          "CREATE TABLE "
              + databaseName
              + ".`"
              + TABLE_NAME
              + "` (id INT NOT NULL, name VARCHAR(64), event_time DATETIME)"
              + " DISTRIBUTED BY HASH(id) BUCKETS 8");
      statement.execute(
          "INSERT INTO "
              + databaseName
              + ".`"
              + TABLE_NAME
              + "` VALUES (1, 'one', '2026-01-01 00:00:00'),"
              + " (2, 'two', '2026-01-02 00:00:00'),"
              + " (3, 'three', '2026-01-01 16:00:00'), (4, 'four', NULL),"
              + " (5, 'five', NULL), (6, 'six', NULL), (7, 'seven', NULL), (8, 'eight', NULL)");
    }
  }

  private String sparkTable() {
    return CATALOG_NAME + "." + databaseName + ".`" + TABLE_NAME + "`";
  }

  @Test
  void testNativePartitionsProjectionAndPredicate() throws Exception {
    assertNativeRead(false);
  }

  @Test
  @Tag("doris-standalone")
  void testNativePartitionsReadOnTwoExecutors() throws Exception {
    assertNativeRead(true);
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
  void testTimestampReadsUseJdbcWhenConnectionTimezoneDiffers() {
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
      String nativeTable = createCatalog("jdbc_doris_native_tz", nativeProperties, TABLE_NAME);
      String jdbcTable = createCatalog("jdbc_doris_baseline_tz", jdbcProperties, TABLE_NAME);
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

  private boolean standalone() {
    return Boolean.getBoolean("gravitino.test.doris.standalone");
  }

  private String readMode() {
    return System.getenv().getOrDefault("GRAVITINO_TEST_DORIS_READ_MODE", "thrift");
  }

  private void assertNativeRead(boolean distributed) throws Exception {
    if (distributed) {
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .until(
              () ->
                  getSparkSession().sparkContext().statusTracker().getExecutorInfos().length >= 3);
    }
    Map<String, String> properties = new HashMap<>(getCatalogConfigs());
    properties.keySet().removeIf(key -> key.startsWith("spark.bypass.doris."));
    String jdbcTable = createCatalog("jdbc_doris_compare", properties, TABLE_NAME);
    String query = "SELECT id, name FROM %s WHERE id >= 3 AND id <= 8";
    Dataset<Row> nativeRows = getSparkSession().sql(String.format(query, sparkTable()));
    Dataset<Row> jdbcRows = getSparkSession().sql(String.format(query, jdbcTable));
    Scan scan = getScan(nativeRows);
    Assertions.assertInstanceOf(DorisScanV2.class, scan);
    assertReaderMode(scan);
    Assertions.assertArrayEquals(new String[] {"id", "name"}, scan.readSchema().fieldNames());
    InputPartition[] partitions = scan.toBatch().planInputPartitions();
    Assertions.assertTrue(partitions.length > 1);
    for (InputPartition partition : partitions) {
      DorisInputPartition doris = Assertions.assertInstanceOf(DorisInputPartition.class, partition);
      Assertions.assertArrayEquals(new String[] {"`id`", "`name`"}, doris.readCols());
      Assertions.assertTrue(
          Arrays.stream(doris.predicates())
              .anyMatch(p -> p.contains("id") && p.contains(">=") && p.contains("3")));
      Assertions.assertTrue(
          Arrays.stream(doris.predicates())
              .anyMatch(p -> p.contains("id") && p.contains("<=") && p.contains("8")));
    }
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
    if (distributed) {
      getSparkSession().sparkContext().addSparkListener(listener);
    }
    List<Row> nativeResult;
    try {
      nativeResult = nativeRows.collectAsList();
      if (distributed) {
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> executors.size() >= 2);
        Assertions.assertFalse(executors.contains("driver"));
      }
    } finally {
      if (distributed) {
        getSparkSession().sparkContext().removeSparkListener(listener);
      }
    }
    Assertions.assertInstanceOf(JDBCScan.class, getScan(jdbcRows));
    Comparator<Row> byId = Comparator.comparingInt(row -> row.getInt(0));
    List<Row> sortedNative = nativeResult.stream().sorted(byId).collect(Collectors.toList());
    List<Row> sortedJdbc =
        jdbcRows.collectAsList().stream().sorted(byId).collect(Collectors.toList());
    Assertions.assertEquals(
        List.of(3, 4, 5, 6, 7, 8),
        sortedNative.stream().map(row -> row.getInt(0)).collect(Collectors.toList()));
    Assertions.assertEquals(sortedNative, sortedJdbc);
  }

  private String createCatalog(String catalog, Map<String, String> properties, String table) {
    client
        .loadMetalake("test")
        .createCatalog(catalog, Catalog.Type.RELATIONAL, "jdbc-doris", "", properties);
    getSparkSession()
        .conf()
        .set("spark.sql.catalog." + catalog, GravitinoDorisCatalogSpark35.class.getName());
    return catalog + "." + databaseName + ".`" + table + "`";
  }

  private void assertReaderMode(Scan scan) throws ReflectiveOperationException {
    Object factory = scan.toBatch().createReaderFactory();
    Field mode = factory.getClass().getDeclaredField("mode");
    mode.setAccessible(true);
    Assertions.assertEquals(readMode().toUpperCase(Locale.ROOT), mode.get(factory).toString());
  }

  private static Scan getScan(Dataset<Row> data) {
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
