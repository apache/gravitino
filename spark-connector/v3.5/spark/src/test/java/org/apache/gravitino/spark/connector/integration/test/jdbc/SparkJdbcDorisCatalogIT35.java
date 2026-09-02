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
import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableSet;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.DorisContainer;
import org.apache.gravitino.integration.test.container.DorisImageName;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.integration.test.SparkEnvIT;
import org.apache.gravitino.spark.connector.jdbc.doris.GravitinoDorisCatalogSpark35;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableWritePrivilege;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Spark 3.5 integration tests for the Gravitino-owned Doris batch read/write adapter. */
@Tag("gravitino-docker-test")
public class SparkJdbcDorisCatalogIT35 extends SparkEnvIT {

  private static final String DORIS_SPARK_CONNECTOR_JAR = "gravitino.doris.spark.connector.jar";
  private static final String SPARK_RUNTIME_JAR_ENV = "GRAVITINO_TEST_SPARK_RUNTIME_JAR";
  private static final String MYSQL_CONNECTOR_JAR_ENV = "GRAVITINO_TEST_MYSQL_CONNECTOR_JAR";
  private static final String DORIS_SINK_ENABLE_2PC = "doris.sink.enable-2pc";
  private static final String CATALOG_NAME = "jdbc_doris";
  private static final String DATABASE_NAME = "doris_spark_it";
  private static final String TABLE_NAME = "read_smoke";
  private static final String SPECIAL_TABLE_NAME = "read_special";
  private static final String NORMALIZED_ORDER_TABLE_NAME = "read_normalized_order";
  private static final String PARTITIONED_CATALOG_NAME = "jdbc_doris_partitioned";
  private static final String APPEND_TABLE_NAME = "write_append";
  private static final String BULK_TABLE_NAME = "write_bulk";
  private static final String TYPES_TABLE_NAME = "write_types";
  private static final String SCHEMA_TABLE_NAME = "write_schema";
  private static final String DDL_BLOCKED_TABLE_NAME = "ddl_blocked";
  private static final String POLICY_TABLE_NAME = "write_policy";
  private static final String TRUNCATE_TABLE_NAME = "write_truncate";
  private static final String TRUNCATE_FAILURE_TABLE_NAME = "write_truncate_failure";
  private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
  private static final String WRITER_USER = "gravitino_spark_writer";
  private static final DorisImageName DORIS_IMAGE = dorisImage();

  private String jdbcUrl;
  private String jdbcUser;
  private String jdbcPassword;
  private int feHttpPort;

  @Override
  protected String getCatalogName() {
    return CATALOG_NAME;
  }

  @Override
  protected String getProvider() {
    return "jdbc-doris";
  }

  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> properties = new HashMap<>();
    properties.put(GRAVITINO_JDBC_URL, jdbcUrl);
    properties.put(GRAVITINO_JDBC_USER, jdbcUser);
    properties.put(GRAVITINO_JDBC_PASSWORD, jdbcPassword);
    properties.put(GRAVITINO_JDBC_DRIVER, JDBC_DRIVER);
    properties.put("credential-providers", JdbcCredential.JDBC_CREDENTIAL_TYPE);
    properties.put("doris-fenodes", "127.0.0.1:" + feHttpPort);
    properties.put("doris-query-port", Integer.toString(getDorisMysqlPort()));
    properties.put("doris-write-mode", "batch");
    properties.put("doris-write-overwrite-mode", "truncate");
    return properties;
  }

  @Override
  protected boolean supportsFunction() {
    return false;
  }

  @Override
  protected void initCatalogEnv() throws Exception {
    ContainerSuite suite = ContainerSuite.getInstance();
    suite.startDorisContainer(DORIS_IMAGE);
    DorisContainer container = suite.getDorisContainer(DORIS_IMAGE);
    jdbcUrl =
        String.format(
            "jdbc:mysql://%s:%d/", container.getContainerIpAddress(), container.getFeMysqlPort());
    feHttpPort = container.getFeHttpPort();
    jdbcUser = WRITER_USER;
    jdbcPassword = "it-" + UUID.randomUUID().toString().replace("-", "");
    try (Connection connection =
            DriverManager.getConnection(
                jdbcUrl, DorisContainer.USER_NAME, DorisContainer.PASSWORD);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME);
      statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + TABLE_NAME);
      statement.execute(
          "CREATE TABLE "
              + DATABASE_NAME
              + "."
              + TABLE_NAME
              + " (id INT, name VARCHAR(64)) DISTRIBUTED BY HASH(id) BUCKETS 1");
      statement.execute(
          "INSERT INTO " + DATABASE_NAME + "." + TABLE_NAME + " VALUES (1, 'one'), (2, 'two')");
      statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + SPECIAL_TABLE_NAME);
      statement.execute(
          "CREATE TABLE "
              + DATABASE_NAME
              + "."
              + SPECIAL_TABLE_NAME
              + " (id INT, large_value LARGEINT, payload JSON) DISTRIBUTED BY HASH(id) BUCKETS 1");
      statement.execute(
          "INSERT INTO "
              + DATABASE_NAME
              + "."
              + SPECIAL_TABLE_NAME
              + " VALUES (1, 9223372036854775808, '{\"kind\":\"special\"}')");
      statement.execute(
          "DROP TABLE IF EXISTS " + DATABASE_NAME + "." + NORMALIZED_ORDER_TABLE_NAME);
      statement.execute(
          "CREATE TABLE "
              + DATABASE_NAME
              + "."
              + NORMALIZED_ORDER_TABLE_NAME
              + " (id INT, large_value LARGEINT) DISTRIBUTED BY HASH(id) BUCKETS 1");
      statement.execute(
          "INSERT INTO "
              + DATABASE_NAME
              + "."
              + NORMALIZED_ORDER_TABLE_NAME
              + " VALUES (1, 9), (2, 10)");
      createWriteTable(statement, APPEND_TABLE_NAME);
      createEmptyWriteTable(statement, BULK_TABLE_NAME, 4);
      statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + TYPES_TABLE_NAME);
      statement.execute(
          "CREATE TABLE "
              + DATABASE_NAME
              + "."
              + TYPES_TABLE_NAME
              + " (id INT NOT NULL, label VARCHAR(64), event_time DATETIME(6), "
              + "amount DECIMAL(18,3)) DISTRIBUTED BY HASH(id) BUCKETS 2");
      statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + SCHEMA_TABLE_NAME);
      statement.execute(
          "CREATE TABLE "
              + DATABASE_NAME
              + "."
              + SCHEMA_TABLE_NAME
              + " (id INT NOT NULL, label VARCHAR(64), event_time DATETIME(6), "
              + "amount DECIMAL(18,3)) DISTRIBUTED BY HASH(id) BUCKETS 1");
      statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + DDL_BLOCKED_TABLE_NAME);
      createWriteTable(statement, POLICY_TABLE_NAME);
      createWriteTable(statement, TRUNCATE_TABLE_NAME);
      statement.execute(
          "DROP TABLE IF EXISTS " + DATABASE_NAME + "." + TRUNCATE_FAILURE_TABLE_NAME);
      statement.execute(
          "CREATE TABLE "
              + DATABASE_NAME
              + "."
              + TRUNCATE_FAILURE_TABLE_NAME
              + " (event_time DATETIME(3)) DISTRIBUTED BY RANDOM BUCKETS 1");
      statement.execute(
          "INSERT INTO "
              + DATABASE_NAME
              + "."
              + TRUNCATE_FAILURE_TABLE_NAME
              + " VALUES ('2026-01-01 00:00:00.000')");
      statement.execute("DROP USER IF EXISTS '" + WRITER_USER + "'");
      statement.execute("CREATE USER '" + WRITER_USER + "' IDENTIFIED BY '" + jdbcPassword + "'");
      statement.execute(
          "GRANT SELECT_PRIV, LOAD_PRIV, DROP_PRIV ON `"
              + DATABASE_NAME
              + "`.* TO '"
              + WRITER_USER
              + "'");
    }
  }

  @Override
  protected void configureSparkConf(SparkConf sparkConf) {
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_ENABLE_DORIS_SUPPORT, "true");
    String dorisConnectorJar = System.getProperty(DORIS_SPARK_CONNECTOR_JAR);
    if ("deploy".equals(System.getProperty("testMode"))) {
      Assertions.assertNotNull(
          dorisConnectorJar,
          "Deploy-mode Doris integration tests require an external Doris Spark Connector JAR");
      sparkConf.set("spark.jars", dorisConnectorJar);
    }
    if (getSparkMaster().startsWith("spark://")) {
      String sparkRuntimeJar = System.getenv(SPARK_RUNTIME_JAR_ENV);
      String mysqlConnectorJar = System.getenv(MYSQL_CONNECTOR_JAR_ENV);
      Assertions.assertNotNull(
          sparkRuntimeJar,
          "Standalone Doris integration tests require the Gravitino Spark runtime JAR");
      Assertions.assertNotNull(
          dorisConnectorJar,
          "Standalone Doris integration tests require an external Doris Spark Connector JAR");
      Assertions.assertNotNull(
          mysqlConnectorJar,
          "Standalone Doris integration tests require an external MySQL Connector/J JAR");
      sparkConf.set(
          "spark.jars", sparkRuntimeJar + "," + dorisConnectorJar + "," + mysqlConnectorJar);
      sparkConf.set("spark.cores.max", "2");
      sparkConf.set("spark.executor.cores", "1");
      sparkConf.set("spark.executor.instances", "2");
      sparkConf.set("spark.driver.bindAddress", "127.0.0.1");
      sparkConf.set("spark.driver.host", "127.0.0.1");
    }
  }

  @Test
  void testCatalogClassName() {
    String className =
        getSparkSession().sessionState().conf().getConfString("spark.sql.catalog." + CATALOG_NAME);
    Assertions.assertEquals(GravitinoDorisCatalogSpark35.class.getName(), className);
  }

  @Test
  void testWriteAwareLoadExposesOnlyGovernedCapabilities() throws Exception {
    Table table =
        ((GravitinoDorisCatalogSpark35)
                getSparkSession().sessionState().catalogManager().catalog(CATALOG_NAME))
            .loadTable(
                Identifier.of(new String[] {DATABASE_NAME}, APPEND_TABLE_NAME),
                ImmutableSet.of(TableWritePrivilege.INSERT, TableWritePrivilege.DELETE));

    Assertions.assertEquals(
        ImmutableSet.of(
            TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.TRUNCATE),
        table.capabilities());
    Assertions.assertFalse(table.capabilities().contains(TableCapability.STREAMING_WRITE));
  }

  @Test
  void testBatchAppendUsesOfficialDorisWriter() throws Exception {
    sql(
        "INSERT INTO "
            + CATALOG_NAME
            + "."
            + DATABASE_NAME
            + "."
            + APPEND_TABLE_NAME
            + " VALUES (2, 'two'), (3, 'three')");

    await()
        .atMost(Duration.ofMinutes(1))
        .untilAsserted(() -> Assertions.assertEquals(3, rowCount(APPEND_TABLE_NAME)));
  }

  @Test
  void testEmptyAndMultiPartitionBatchWrites() throws Exception {
    Dataset<Row> input =
        getSparkSession()
            .range(100_000)
            .repartition(4)
            .selectExpr("CAST(id AS INT) AS id", "CAST(CONCAT('row-', id) AS STRING) AS name");
    String table = CATALOG_NAME + "." + DATABASE_NAME + "." + BULK_TABLE_NAME;

    input.limit(0).writeTo(table).append();
    Assertions.assertEquals(0, rowCount(BULK_TABLE_NAME));

    input.writeTo(table).append();
    await()
        .atMost(Duration.ofMinutes(2))
        .untilAsserted(
            () -> {
              Assertions.assertEquals(100_000, rowCount(BULK_TABLE_NAME));
              Assertions.assertEquals(4_999_950_000L, sumIds(BULK_TABLE_NAME));
            });
  }

  @Test
  void testNullableDecimalAndDatetimeWrites() throws Exception {
    sql(
        "INSERT INTO "
            + CATALOG_NAME
            + "."
            + DATABASE_NAME
            + "."
            + TYPES_TABLE_NAME
            + " VALUES "
            + "(10, 'alpha', '2026-09-01 12:34:56.123456', 12.345), "
            + "(11, NULL, NULL, NULL)");

    await()
        .atMost(Duration.ofMinutes(1))
        .untilAsserted(
            () ->
                Assertions.assertEquals(
                    List.of(
                        "10,alpha,2026-09-01 12:34:56.123456,12.345", "11,<null>,<null>,<null>"),
                    typedRows()));
  }

  @Test
  void testPartitionedJdbcReadUsesFourPartitions() {
    ensurePartitionedCatalog();
    Dataset<Row> rows =
        getSparkSession()
            .table(PARTITIONED_CATALOG_NAME + "." + DATABASE_NAME + "." + SPECIAL_TABLE_NAME)
            .select("id", "large_value", "payload");

    Assertions.assertEquals(4, rows.rdd().getNumPartitions());
    Assertions.assertTrue(rows.queryExecution().executedPlan().toString().contains("JDBCScan"));
    List<Row> collected = rows.collectAsList();
    Assertions.assertEquals(1, collected.size());
    Assertions.assertEquals(1, collected.get(0).getInt(0));
    Assertions.assertEquals("9223372036854775808", collected.get(0).getString(1));
    Assertions.assertEquals("{\"kind\":\"special\"}", collected.get(0).getString(2));
  }

  @Test
  void testSchemaMismatchFailsBeforeWriting() throws Exception {
    StructType wrongName =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("label", DataTypes.StringType, false);
    Assertions.assertThrows(
        Exception.class,
        () ->
            getSparkSession()
                .createDataFrame(
                    Collections.singletonList(RowFactory.create(20, "blocked")), wrongName)
                .writeTo(CATALOG_NAME + "." + DATABASE_NAME + "." + POLICY_TABLE_NAME)
                .append());

    StructType wrongType =
        new StructType()
            .add("id", DataTypes.BinaryType, false)
            .add("name", DataTypes.StringType, false);
    Assertions.assertThrows(
        Exception.class,
        () ->
            getSparkSession()
                .createDataFrame(
                    Collections.singletonList(RowFactory.create(new byte[] {2, 1}, "blocked")),
                    wrongType)
                .writeTo(CATALOG_NAME + "." + DATABASE_NAME + "." + POLICY_TABLE_NAME)
                .append());

    StructType unsafeNullability =
        new StructType()
            .add("id", DataTypes.IntegerType, true)
            .add("label", DataTypes.StringType, true)
            .add("event_time", DataTypes.StringType, true)
            .add("amount", DataTypes.createDecimalType(18, 3), true);
    Assertions.assertThrows(
        Exception.class,
        () ->
            getSparkSession()
                .createDataFrame(
                    Collections.singletonList(
                        RowFactory.create(
                            null,
                            "blocked",
                            "2026-09-01 12:34:56.123456",
                            new BigDecimal("22.000"))),
                    unsafeNullability)
                .writeTo(CATALOG_NAME + "." + DATABASE_NAME + "." + SCHEMA_TABLE_NAME)
                .append());

    Assertions.assertEquals(1, rowCount(POLICY_TABLE_NAME));
    Assertions.assertEquals(0, rowCount(SCHEMA_TABLE_NAME));
  }

  @Test
  void testStandaloneUsesTwoWorkers() {
    Assumptions.assumeTrue(getSparkMaster().startsWith("spark://"));

    await()
        .atMost(Duration.ofMinutes(1))
        .untilAsserted(
            () ->
                Assertions.assertTrue(
                    getSparkSession().sparkContext().statusTracker().getExecutorInfos().length >= 3,
                    "Standalone coverage requires two worker executors plus the driver"));
  }

  @Test
  void testExplicitTruncateOverwriteIsNonAppend() throws Exception {
    sql(
        "INSERT OVERWRITE TABLE "
            + CATALOG_NAME
            + "."
            + DATABASE_NAME
            + "."
            + TRUNCATE_TABLE_NAME
            + " VALUES (9, 'replacement')");

    await()
        .atMost(Duration.ofMinutes(1))
        .untilAsserted(
            () -> {
              Assertions.assertEquals(1, rowCount(TRUNCATE_TABLE_NAME));
              Assertions.assertEquals(9, singleId(TRUNCATE_TABLE_NAME));
            });
  }

  @Test
  void testPerWriteOptionsCannotOverrideGovernedPolicy() throws Exception {
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("name", DataTypes.StringType, false);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            getSparkSession()
                .createDataFrame(Collections.singletonList(RowFactory.create(4, "blocked")), schema)
                .writeTo(CATALOG_NAME + "." + DATABASE_NAME + "." + POLICY_TABLE_NAME)
                .option(DORIS_SINK_ENABLE_2PC, "false")
                .append());
    Assertions.assertEquals(1, rowCount(POLICY_TABLE_NAME));
  }

  @Test
  void testPerReadOptionsCannotOverrideGovernedConnection() {
    String secret = "unmanaged-secret-" + UUID.randomUUID();
    Exception failure =
        Assertions.assertThrows(
            Exception.class,
            () ->
                getSparkSession()
                    .read()
                    .option("url", "jdbc:mysql://127.0.0.1:1/unmanaged")
                    .option("user", "unmanaged-user")
                    .option("password", secret)
                    .option("dbtable", "unmanaged_table")
                    .table(CATALOG_NAME + "." + DATABASE_NAME + "." + TABLE_NAME)
                    .count());

    assertCauseContains(failure, "Doris per-read options cannot override governed catalog policy");
    assertCauseDoesNotContain(failure, secret);
  }

  @Test
  void testUnsupportedMutationPathsLeaveDataUnchanged() throws Exception {
    String table = CATALOG_NAME + "." + DATABASE_NAME + "." + POLICY_TABLE_NAME;
    StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("name", DataTypes.StringType, false);
    Dataset<Row> input =
        getSparkSession()
            .createDataFrame(Collections.singletonList(RowFactory.create(30, "blocked")), schema);

    Assertions.assertThrows(
        Exception.class, () -> input.writeTo(table).overwrite(functions.col("id").equalTo(1)));
    Assertions.assertThrows(Exception.class, () -> input.writeTo(table).overwritePartitions());
    Assertions.assertThrows(Exception.class, () -> sql("DELETE FROM " + table + " WHERE id = 1"));
    Assertions.assertThrows(
        Exception.class, () -> sql("UPDATE " + table + " SET name = 'blocked' WHERE id = 1"));
    Assertions.assertThrows(
        Exception.class,
        () ->
            sql(
                "MERGE INTO "
                    + table
                    + " AS target USING (SELECT 1 AS id, 'blocked' AS name) AS source "
                    + "ON target.id = source.id WHEN MATCHED THEN UPDATE SET name = source.name"));
    Assertions.assertThrows(
        Exception.class, () -> sql("ALTER TABLE " + table + " ADD COLUMN blocked INT"));
    Assertions.assertThrows(Exception.class, () -> sql("DROP TABLE " + table));
    Assertions.assertThrows(
        Exception.class,
        () ->
            sql(
                "CREATE TABLE "
                    + CATALOG_NAME
                    + "."
                    + DATABASE_NAME
                    + "."
                    + DDL_BLOCKED_TABLE_NAME
                    + " AS SELECT 1 AS id"));

    Assertions.assertEquals(1, rowCount(POLICY_TABLE_NAME));
    Assertions.assertEquals(1, singleId(POLICY_TABLE_NAME));
    Assertions.assertFalse(physicalTableExists(DDL_BLOCKED_TABLE_NAME));
  }

  @Test
  void testStreamingWriteIsRejectedWithoutMutation(@TempDir Path tempDir) throws Exception {
    Dataset<Row> stream =
        getSparkSession()
            .readStream()
            .format("rate")
            .load()
            .selectExpr(
                "CAST(value AS INT) AS id", "CAST(CONCAT('row-', value) AS STRING) AS name");

    Assertions.assertThrows(
        Exception.class,
        () ->
            stream
                .writeStream()
                .option("checkpointLocation", tempDir.resolve("checkpoint").toString())
                .toTable(CATALOG_NAME + "." + DATABASE_NAME + "." + POLICY_TABLE_NAME));
    Assertions.assertEquals(1, rowCount(POLICY_TABLE_NAME));
  }

  @Test
  void testTruncateThenInvalidLoadLeavesDocumentedEmptyTable() throws Exception {
    SparkException failure =
        Assertions.assertThrows(
            SparkException.class,
            () ->
                sql(
                    "INSERT OVERWRITE TABLE "
                        + CATALOG_NAME
                        + "."
                        + DATABASE_NAME
                        + "."
                        + TRUNCATE_FAILURE_TABLE_NAME
                        + " VALUES ('not-a-datetime')"));

    assertCauseContains(
        failure, "Doris DATETIME input does not match the certified precision-specific format");
    Assertions.assertEquals(0, rowCount(TRUNCATE_FAILURE_TABLE_NAME));
  }

  @Test
  void testNormalizedNumericColumnsUseSparkStringAndCoercionSemantics() {
    String table = CATALOG_NAME + "." + DATABASE_NAME + "." + NORMALIZED_ORDER_TABLE_NAME;

    List<Object[]> lexicalOrder = sql("SELECT large_value FROM " + table + " ORDER BY large_value");
    Assertions.assertEquals(List.of("10", "9"), firstColumnStrings(lexicalOrder));

    List<Object[]> stringRange =
        sql("SELECT large_value FROM " + table + " WHERE large_value > '9'");
    Assertions.assertTrue(stringRange.isEmpty());

    List<Object[]> implicitNumericRange =
        sql("SELECT large_value FROM " + table + " WHERE large_value > 9");
    Assertions.assertEquals(List.of("10"), firstColumnStrings(implicitNumericRange));

    List<Object[]> explicitNumericRange =
        sql("SELECT large_value FROM " + table + " WHERE CAST(large_value AS DECIMAL(38, 0)) > 9");
    Assertions.assertEquals(List.of("10"), firstColumnStrings(explicitNumericRange));
  }

  @Test
  void testSpecialTypesUseStringNormalizationContract() throws Exception {
    try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT CAST(`large_value` AS STRING), CAST(`payload` AS STRING) FROM `"
                    + DATABASE_NAME
                    + "`.`"
                    + SPECIAL_TABLE_NAME
                    + "`")) {
      Assertions.assertTrue(resultSet.next());
      Assertions.assertEquals("9223372036854775808", resultSet.getString(1));
      Assertions.assertEquals("{\"kind\":\"special\"}", resultSet.getString(2));
    }

    List<Object[]> rows =
        sql(
            "SELECT large_value, payload FROM "
                + CATALOG_NAME
                + "."
                + DATABASE_NAME
                + "."
                + SPECIAL_TABLE_NAME);

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("9223372036854775808", rows.get(0)[0]);
    Assertions.assertEquals("{\"kind\":\"special\"}", rows.get(0)[1]);
  }

  @Test
  void testDeployProvidesDorisConnectorAsExternalSparkJar() {
    Assumptions.assumeTrue("deploy".equals(System.getProperty("testMode")));
    String configuredJars = getSparkSession().sparkContext().conf().get("spark.jars");
    Assertions.assertTrue(
        configuredJars.contains(System.getProperty(DORIS_SPARK_CONNECTOR_JAR)),
        "Deploy mode must provide the Doris connector through spark.jars");
  }

  @Test
  void testReadScalarTable() {
    List<Object[]> rows =
        sql(
            "SELECT id, name FROM "
                + CATALOG_NAME
                + "."
                + DATABASE_NAME
                + "."
                + TABLE_NAME
                + " ORDER BY id");

    Assertions.assertEquals(2, rows.size());
    Assertions.assertEquals(1, rows.get(0)[0]);
    Assertions.assertEquals("one", rows.get(0)[1]);
    Assertions.assertEquals(2, rows.get(1)[0]);
    Assertions.assertEquals("two", rows.get(1)[1]);
  }

  @Test
  void testNativeFilter() {
    List<Object[]> rows =
        sql(
            "SELECT id, name FROM "
                + CATALOG_NAME
                + "."
                + DATABASE_NAME
                + "."
                + TABLE_NAME
                + " WHERE id = 2");

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(2, rows.get(0)[0]);
    Assertions.assertEquals("two", rows.get(0)[1]);
  }

  @Test
  void testAggregateUsesJdbcSemantics() {
    List<Object[]> rows =
        sql("SELECT COUNT(*) FROM " + CATALOG_NAME + "." + DATABASE_NAME + "." + TABLE_NAME);

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(2L, ((Number) rows.get(0)[0]).longValue());
  }

  @Test
  void testTopNAndGlobalLimit() {
    List<Object[]> rows =
        sql(
            "SELECT id, name FROM "
                + CATALOG_NAME
                + "."
                + DATABASE_NAME
                + "."
                + TABLE_NAME
                + " ORDER BY id DESC LIMIT 1");

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(2, rows.get(0)[0]);
    Assertions.assertEquals("two", rows.get(0)[1]);
  }

  @Test
  void testOffset() {
    List<Object[]> rows =
        sql(
            "SELECT id, name FROM "
                + CATALOG_NAME
                + "."
                + DATABASE_NAME
                + "."
                + TABLE_NAME
                + " ORDER BY id LIMIT 1 OFFSET 1");

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(2, rows.get(0)[0]);
    Assertions.assertEquals("two", rows.get(0)[1]);
  }

  private int getDorisMysqlPort() {
    return ContainerSuite.getInstance().getDorisContainer(DORIS_IMAGE).getFeMysqlPort();
  }

  private void ensurePartitionedCatalog() {
    GravitinoMetalake metalake = client.loadMetalake("test");
    if (!metalake.catalogExists(PARTITIONED_CATALOG_NAME)) {
      Map<String, String> properties = new HashMap<>(getCatalogConfigs());
      properties.put("doris-jdbc-partition-column", "id");
      properties.put("doris-jdbc-lower-bound", "0");
      properties.put("doris-jdbc-upper-bound", "100000");
      properties.put("doris-jdbc-num-partitions", "4");
      metalake.createCatalog(
          PARTITIONED_CATALOG_NAME,
          Catalog.Type.RELATIONAL,
          getProvider(),
          "Partitioned Doris Spark integration catalog",
          properties);
    }
    getSparkSession()
        .conf()
        .set(
            "spark.sql.catalog." + PARTITIONED_CATALOG_NAME,
            GravitinoDorisCatalogSpark35.class.getName());
  }

  private void createWriteTable(Statement statement, String tableName) throws Exception {
    createEmptyWriteTable(statement, tableName, 1);
    statement.execute("INSERT INTO " + DATABASE_NAME + "." + tableName + " VALUES (1, 'baseline')");
  }

  private void createEmptyWriteTable(Statement statement, String tableName, int buckets)
      throws Exception {
    statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + tableName);
    statement.execute(
        "CREATE TABLE "
            + DATABASE_NAME
            + "."
            + tableName
            + " (id INT, name VARCHAR(64)) DISTRIBUTED BY HASH(id) BUCKETS "
            + buckets);
  }

  private int rowCount(String tableName) throws Exception {
    try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT COUNT(*) FROM " + DATABASE_NAME + "." + tableName)) {
      Assertions.assertTrue(resultSet.next());
      return resultSet.getInt(1);
    }
  }

  private List<String> typedRows() throws Exception {
    List<String> rows = new ArrayList<>();
    try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT id, label, event_time, amount FROM "
                    + DATABASE_NAME
                    + "."
                    + TYPES_TABLE_NAME
                    + " ORDER BY id")) {
      while (resultSet.next()) {
        rows.add(
            resultSet.getInt(1)
                + ","
                + nullable(resultSet.getString(2))
                + ","
                + nullable(resultSet.getString(3))
                + ","
                + nullable(resultSet.getString(4)));
      }
    }
    return rows;
  }

  private boolean physicalTableExists(String tableName) throws Exception {
    try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '"
                    + DATABASE_NAME
                    + "' AND table_name = '"
                    + tableName
                    + "'")) {
      Assertions.assertTrue(resultSet.next());
      return resultSet.getInt(1) > 0;
    }
  }

  private int singleId(String tableName) throws Exception {
    try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT id FROM " + DATABASE_NAME + "." + tableName)) {
      Assertions.assertTrue(resultSet.next());
      int id = resultSet.getInt(1);
      Assertions.assertFalse(resultSet.next());
      return id;
    }
  }

  private long sumIds(String tableName) throws Exception {
    try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT SUM(id) FROM " + DATABASE_NAME + "." + tableName)) {
      Assertions.assertTrue(resultSet.next());
      return resultSet.getLong(1);
    }
  }

  private static String nullable(String value) {
    return value == null ? "<null>" : value;
  }

  private static List<String> firstColumnStrings(List<Object[]> rows) {
    List<String> values = new ArrayList<>(rows.size());
    rows.forEach(row -> values.add((String) row[0]));
    return values;
  }

  private static void assertCauseContains(Throwable failure, String expected) {
    Throwable current = failure;
    while (current != null) {
      if (String.valueOf(current.getMessage()).contains(expected)) {
        return;
      }
      current = current.getCause();
    }
    Assertions.fail("Expected failure chain to contain: " + expected);
  }

  private static void assertCauseDoesNotContain(Throwable failure, String value) {
    Throwable current = failure;
    while (current != null) {
      Assertions.assertFalse(String.valueOf(current.getMessage()).contains(value));
      current = current.getCause();
    }
  }

  private static DorisImageName dorisImage() {
    String version = System.getenv().getOrDefault("GRAVITINO_TEST_DORIS_VERSION", "3.0.6.2");
    if ("4.0.6".equals(version)) {
      return DorisImageName.VERSION_4_0;
    }
    if ("3.0.6.2".equals(version)) {
      return DorisImageName.VERSION_3_0;
    }
    throw new IllegalArgumentException("Unsupported Doris integration-test version: " + version);
  }
}
