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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.DorisContainer;
import org.apache.gravitino.integration.test.container.DorisImageName;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.integration.test.SparkEnvIT;
import org.apache.gravitino.spark.connector.jdbc.GravitinoJdbcCatalogSpark35;
import org.apache.gravitino.spark.connector.jdbc.doris.GravitinoDorisCatalogSpark35;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.execution.datasources.v2.V1ScanWrapper;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.collection.Iterator;

/** Spark 3.5 integration tests for the governed Doris read baseline. */
@Tag("gravitino-docker-test")
@Tag("doris-multi-version")
public class SparkJdbcDorisReadBaselineIT35 extends SparkEnvIT {

  private static final String CATALOG_NAME = "jdbc_doris";
  private static final String DATABASE_NAME = "doris_spark_baseline_it";
  private static final String TABLE_NAME = "read_baseline";
  private static final String ORDINARY_TYPES_TABLE_NAME = "read_ordinary_types";
  private static final String PATTERN_MATCH_TABLE_NAME = "readxbaseline";
  private static final String OTHER_DATABASE_NAME = "doris_spark_baseline_other_it";
  private static final String JSON_TABLE_NAME = "read_json";
  private static final String UNSUPPORTED_TABLE_NAME = "unsupported_char";
  private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
  private static final DorisImageName DORIS_IMAGE = DorisImageName.VERSION_3_0;

  private String jdbcUrl;
  private String jdbcUser;
  private String jdbcPassword;

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
    properties.put("spark.bypass.pushDownPredicate", "false");
    properties.put("spark.bypass.connectTimeout", "10000");
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
    jdbcUser = DorisContainer.USER_NAME;
    jdbcPassword = DorisContainer.PASSWORD;
    jdbcUrl =
        String.format(
            "jdbc:mysql://%s:%d/", container.getContainerIpAddress(), container.getFeMysqlPort());
    executeJdbcSql(
        "CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME,
        "DROP TABLE IF EXISTS " + dorisTable(TABLE_NAME),
        "CREATE TABLE "
            + dorisTable(TABLE_NAME)
            + " (id INT NOT NULL, name VARCHAR(64)) DISTRIBUTED BY HASH(id) BUCKETS 1",
        "DROP TABLE IF EXISTS " + dorisTable(JSON_TABLE_NAME),
        "CREATE TABLE "
            + dorisTable(JSON_TABLE_NAME)
            + " (id INT NOT NULL, payload JSON) DISTRIBUTED BY HASH(id) BUCKETS 1",
        "DROP TABLE IF EXISTS " + dorisTable(UNSUPPORTED_TABLE_NAME),
        "CREATE TABLE "
            + dorisTable(UNSUPPORTED_TABLE_NAME)
            + " (id INT NOT NULL, value CHAR(4)) DISTRIBUTED BY HASH(id) BUCKETS 1",
        "INSERT INTO " + dorisTable(TABLE_NAME) + " VALUES (1, 'one'), (2, 'two')");
  }

  @Override
  protected Map<String, String> getExtraSparkConfigs() {
    return Map.of(
        GravitinoSparkConfig.GRAVITINO_ENABLE_DORIS_SUPPORT,
        "true",
        "spark.sql.catalog." + CATALOG_NAME + ".connectTimeout",
        "20000");
  }

  @Test
  void testCatalogClassName() {
    String className =
        getSparkSession().sessionState().conf().getConfString("spark.sql.catalog." + CATALOG_NAME);
    Assertions.assertEquals(GravitinoDorisCatalogSpark35.class.getName(), className);
  }

  @Test
  void testReadScalarTable() {
    List<Object[]> rows = sql("SELECT id, name FROM " + sparkTable(TABLE_NAME) + " ORDER BY id");

    Assertions.assertEquals(2, rows.size());
    Assertions.assertEquals(1, rows.get(0)[0]);
    Assertions.assertEquals("one", rows.get(0)[1]);
    Assertions.assertEquals(2, rows.get(1)[0]);
    Assertions.assertEquals("two", rows.get(1)[1]);
  }

  @Test
  void testReadJsonColumn() throws Exception {
    executeJdbcSql(
        "INSERT INTO "
            + dorisTable(JSON_TABLE_NAME)
            + " VALUES (1, '{\"key\":\"value\",\"nested\":{\"n\":1}}'),"
            + " (2, '[1,2]'), (3, '42'), (4, '\"text\"'), (5, 'null'), (6, NULL)");
    try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        PreparedStatement statement =
            connection.prepareStatement(
                "INSERT INTO " + dorisTable(JSON_TABLE_NAME) + " VALUES (?, ?)")) {
      statement.setInt(1, 7);
      statement.setString(2, "{\"quote\":\"a\\\"b\",\"unicode\":\"\\u4e2d\"}");
      statement.executeUpdate();
    }
    List<Row> expected = new ArrayList<>();
    try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        Statement statement = connection.createStatement();
        ResultSet result =
            statement.executeQuery(
                "SELECT id, payload FROM " + dorisTable(JSON_TABLE_NAME) + " ORDER BY id")) {
      while (result.next()) {
        expected.add(RowFactory.create(result.getInt(1), result.getString(2)));
      }
    }
    Dataset<Row> result =
        getSparkSession()
            .sql("SELECT id, payload FROM " + sparkTable(JSON_TABLE_NAME) + " ORDER BY id");
    Assertions.assertEquals(DataTypes.StringType, result.schema().fields()[1].dataType());
    Assertions.assertEquals(7, expected.size());
    Assertions.assertEquals(expected, result.collectAsList());
    Assertions.assertTrue(expected.get(5).isNullAt(1));

    SparkSession pushdownSession = getSparkSession().newSession();
    pushdownSession.conf().set("spark.sql.catalog." + CATALOG_NAME + ".pushDownPredicate", "true");
    Dataset<Row> filtered =
        pushdownSession.sql(
            "SELECT id FROM "
                + sparkTable(JSON_TABLE_NAME)
                + " WHERE payload = '"
                + expected.get(1).getString(1)
                + "' ORDER BY id LIMIT 1");
    Assertions.assertEquals(List.of(RowFactory.create(2)), filtered.collectAsList());
    JDBCScan scan = jdbcScan(filtered);
    Assertions.assertTrue(scan.relation().jdbcOptions().pushDownPredicate());
    Assertions.assertEquals(0, scan.pushedPredicates().length);
    Assertions.assertTrue(filtered.queryExecution().optimizedPlan().toString().contains("Filter"));
  }

  @Test
  void testReadFloatRangeAndNull() throws Exception {
    String tableName = "read_float_range";
    try {
      executeJdbcSql(
          "CREATE TABLE "
              + dorisTable(tableName)
              + " (id INT NOT NULL, value FLOAT) DISTRIBUTED BY HASH(id) BUCKETS 1",
          "INSERT INTO "
              + dorisTable(tableName)
              + " VALUES (1, -1e30), (2, 1e-30), (3, 0), (4, 1e30), (5, NULL),"
              + " (6, 0.1), (7, 0.10000001)");
      Dataset<Row> result =
          getSparkSession().sql("SELECT id, value FROM " + sparkTable(tableName) + " ORDER BY id");
      Assertions.assertEquals(DataTypes.FloatType, result.schema().fields()[1].dataType());
      Assertions.assertEquals(
          DataTypes.FloatType, jdbcScan(result).readSchema().fields()[1].dataType());
      Assertions.assertEquals(
          List.of(
              RowFactory.create(1, -1e30f),
              RowFactory.create(2, 1e-30f),
              RowFactory.create(3, 0.0f),
              RowFactory.create(4, 1e30f),
              RowFactory.create(5, null),
              RowFactory.create(6, 0.1f),
              RowFactory.create(7, 0.10000001f)),
          result.collectAsList());
      Assertions.assertEquals(
          List.of(RowFactory.create(1)),
          getSparkSession()
              .sql("SELECT id FROM " + sparkTable(tableName) + " WHERE value < 0")
              .collectAsList());
      Dataset<Row> aggregate =
          getSparkSession().sql("SELECT MIN(value), MAX(value) FROM " + sparkTable(tableName));
      Assertions.assertEquals(List.of(RowFactory.create(-1e30f, 1e30f)), aggregate.collectAsList());

      SparkSession pushed = getSparkSession().newSession();
      SparkSession local = getSparkSession().newSession();
      pushed.conf().set("spark.sql.catalog." + CATALOG_NAME + ".pushDownPredicate", "true");
      local.conf().set("spark.sql.catalog." + CATALOG_NAME + ".pushDownPredicate", "false");
      for (String operator : List.of("=", "<>")) {
        String query =
            "SELECT id FROM "
                + sparkTable(tableName)
                + " WHERE value "
                + operator
                + " CAST(0.1 AS FLOAT) AND id >= 1 ORDER BY id";
        Dataset<Row> pushedResult = pushed.sql(query);
        List<Row> rows = pushedResult.collectAsList();
        Assertions.assertEquals(local.sql(query).collectAsList(), rows, operator);
        Assertions.assertEquals(
            operator.equals("=")
                ? List.of(RowFactory.create(6))
                : List.of(
                    RowFactory.create(1),
                    RowFactory.create(2),
                    RowFactory.create(3),
                    RowFactory.create(4),
                    RowFactory.create(7)),
            rows,
            operator);
        JDBCScan scan = jdbcScan(pushedResult);
        Assertions.assertTrue(scan.relation().jdbcOptions().pushDownPredicate());
        Assertions.assertTrue(
            Arrays.stream(scan.pushedPredicates())
                .anyMatch(
                    predicate -> predicate.toString().toLowerCase(Locale.ROOT).contains("id")));
        Assertions.assertFalse(
            Arrays.stream(scan.pushedPredicates())
                .anyMatch(
                    predicate ->
                        predicate.toString().toLowerCase(Locale.ROOT).contains("value")
                            && !predicate.name().equals("IS_NULL")
                            && !predicate.name().equals("IS_NOT_NULL")));
      }
    } finally {
      executeJdbcSql("DROP TABLE IF EXISTS " + dorisTable(tableName));
    }
  }

  @Test
  void testCatalogBypassReadOptionControlsPredicatePushdown() {
    Dataset<Row> result =
        getSparkSession().sql("SELECT id, name FROM " + sparkTable(TABLE_NAME) + " WHERE id = 1");
    List<Row> rows = result.collectAsList();

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(1, rows.get(0).getInt(0));
    Assertions.assertEquals("one", rows.get(0).getString(1));
    JDBCScan jdbcScan = jdbcScan(result);
    Assertions.assertEquals(0, jdbcScan.pushedPredicates().length);
    Assertions.assertTrue(result.queryExecution().optimizedPlan().toString().contains("Filter"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("ordinaryScalarTypes")
  void testReadOrdinaryScalarTypeFamiliesAndNullValues(
      String sqlType, String sqlValue, Object expectedValue) throws Exception {
    int parameters = sqlType.indexOf('(');
    String typeName = parameters < 0 ? sqlType : sqlType.substring(0, parameters);
    String tableName = ORDINARY_TYPES_TABLE_NAME + "_" + typeName.toLowerCase(Locale.ROOT);
    try {
      try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
          Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE IF EXISTS " + dorisTable(tableName));
        statement.execute(
            "CREATE TABLE "
                + dorisTable(tableName)
                + " (id INT NOT NULL, required_value "
                + sqlType
                + " NOT NULL, nullable_value "
                + sqlType
                + " NULL) DISTRIBUTED BY HASH(id) BUCKETS 1");
        statement.execute(
            "INSERT INTO "
                + dorisTable(tableName)
                + " VALUES (1, "
                + sqlValue
                + ", "
                + sqlValue
                + "), (2, "
                + sqlValue
                + ", NULL)");
        if (expectedValue instanceof Timestamp) {
          try (ResultSet resultSet =
              statement.executeQuery(
                  "SELECT required_value FROM " + dorisTable(tableName) + " WHERE id = 1")) {
            Assertions.assertTrue(resultSet.next());
            // Compare raw timestamps with direct JDBC, independently of Spark string rendering.
            expectedValue = resultSet.getTimestamp(1);
            Assertions.assertNotNull(expectedValue);
          }
        }
      }

      GravitinoDorisCatalogSpark35 sparkCatalog =
          (GravitinoDorisCatalogSpark35)
              getSparkSession().sessionState().catalogManager().catalog(CATALOG_NAME);
      sparkCatalog.invalidateTable(Identifier.of(new String[] {DATABASE_NAME}, tableName));
      Dataset<Row> result =
          getSparkSession()
              .sql(
                  "SELECT id, required_value, nullable_value FROM "
                      + sparkTable(tableName)
                      + " ORDER BY id");
      Assertions.assertFalse(result.schema().fields()[1].nullable(), sqlType);
      Assertions.assertTrue(result.schema().fields()[2].nullable(), sqlType);
      List<Row> rows = result.collectAsList();

      Assertions.assertEquals(2, rows.size());
      Assertions.assertEquals(1, rows.get(0).getInt(0));
      Assertions.assertEquals(expectedValue, rows.get(0).get(1), sqlType);
      Assertions.assertEquals(expectedValue, rows.get(0).get(2), sqlType);
      Assertions.assertEquals(2, rows.get(1).getInt(0));
      Assertions.assertEquals(expectedValue, rows.get(1).get(1), sqlType);
      Assertions.assertTrue(rows.get(1).isNullAt(2), sqlType);
    } finally {
      executeJdbcSql("DROP TABLE IF EXISTS " + dorisTable(tableName));
    }
  }

  @Test
  void testSmallIntMatchesGenericJdbcPath() throws Exception {
    String tableName = ORDINARY_TYPES_TABLE_NAME + "_smallint";
    try {
      executeJdbcSql(
          "DROP TABLE IF EXISTS " + dorisTable(tableName),
          "CREATE TABLE "
              + dorisTable(tableName)
              + " (id INT NOT NULL, required_value SMALLINT NOT NULL, "
              + "nullable_value SMALLINT NULL) DISTRIBUTED BY HASH(id) BUCKETS 1",
          "INSERT INTO "
              + dorisTable(tableName)
              + " VALUES (1, -32768, -32768), (2, 32767, 32767), "
              + "(3, -32768, NULL), (4, 32767, NULL)");

      // Isolate catalog registration while sharing the fixture-owned SparkContext.
      SparkSession genericSession = getSparkSession().newSession();
      genericSession
          .conf()
          .set("spark.sql.catalog." + CATALOG_NAME, GravitinoJdbcCatalogSpark35.class.getName());
      List<Row> expectedRows =
          List.of(
              RowFactory.create(1, Short.MIN_VALUE, Short.MIN_VALUE),
              RowFactory.create(2, Short.MAX_VALUE, Short.MAX_VALUE),
              RowFactory.create(3, Short.MIN_VALUE, null),
              RowFactory.create(4, Short.MAX_VALUE, null));
      for (SparkSession session : List.of(genericSession, getSparkSession())) {
        Dataset<Row> result =
            session.sql(
                "SELECT id, required_value, nullable_value FROM "
                    + sparkTable(tableName)
                    + " ORDER BY id");
        Assertions.assertEquals(DataTypes.ShortType, result.schema().fields()[1].dataType());
        Assertions.assertEquals(DataTypes.ShortType, result.schema().fields()[2].dataType());
        Assertions.assertFalse(result.schema().fields()[1].nullable());
        Assertions.assertTrue(result.schema().fields()[2].nullable());
        List<Row> rows = result.collectAsList();
        Assertions.assertEquals(expectedRows, rows);
        for (Row row : rows) {
          Assertions.assertTrue(row.get(1) instanceof Short);
          if (!row.isNullAt(2)) {
            Assertions.assertTrue(row.get(2) instanceof Short);
          }
        }
      }
      // Resolve through each session's SQL context before inspecting the cached catalogs.
      Assertions.assertEquals(
          GravitinoJdbcCatalogSpark35.class,
          genericSession.sessionState().catalogManager().catalog(CATALOG_NAME).getClass());
      Assertions.assertEquals(
          GravitinoDorisCatalogSpark35.class,
          getSparkSession().sessionState().catalogManager().catalog(CATALOG_NAME).getClass());
    } finally {
      executeJdbcSql("DROP TABLE IF EXISTS " + dorisTable(tableName));
    }
  }

  @Test
  void testReadIgnoresColumnsFromOtherTablesAndDatabases() throws Exception {
    String otherTable = OTHER_DATABASE_NAME + "." + TABLE_NAME;
    try {
      executeJdbcSql(
          "CREATE DATABASE IF NOT EXISTS " + OTHER_DATABASE_NAME,
          "DROP TABLE IF EXISTS " + otherTable,
          "CREATE TABLE "
              + otherTable
              + " (unrelated INT) DISTRIBUTED BY HASH(unrelated) BUCKETS 1",
          "DROP TABLE IF EXISTS " + dorisTable(PATTERN_MATCH_TABLE_NAME),
          "CREATE TABLE "
              + dorisTable(PATTERN_MATCH_TABLE_NAME)
              + " (unrelated INT) DISTRIBUTED BY HASH(unrelated) BUCKETS 1");

      GravitinoDorisCatalogSpark35 sparkCatalog =
          (GravitinoDorisCatalogSpark35)
              getSparkSession().sessionState().catalogManager().catalog(CATALOG_NAME);
      sparkCatalog.invalidateTable(Identifier.of(new String[] {DATABASE_NAME}, TABLE_NAME));
      List<Object[]> rows = sql("SELECT id, name FROM " + sparkTable(TABLE_NAME) + " ORDER BY id");
      Assertions.assertEquals(2, rows.size());
      Assertions.assertEquals(1, rows.get(0)[0]);
      Assertions.assertEquals("one", rows.get(0)[1]);
      Assertions.assertEquals(2, rows.get(1)[0]);
      Assertions.assertEquals("two", rows.get(1)[1]);
    } finally {
      executeJdbcSql(
          "DROP TABLE IF EXISTS " + dorisTable(PATTERN_MATCH_TABLE_NAME),
          "DROP TABLE IF EXISTS " + otherTable,
          "DROP DATABASE IF EXISTS " + OTHER_DATABASE_NAME);
    }
  }

  @Test
  void testWriteAwareLoadIsRejected() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            ((GravitinoDorisCatalogSpark35)
                    getSparkSession().sessionState().catalogManager().catalog(CATALOG_NAME))
                .loadTable(
                    Identifier.of(new String[] {DATABASE_NAME}, TABLE_NAME),
                    Collections.emptySet()));
  }

  @Test
  void testUnsupportedPhysicalTypeIsRejected() throws Exception {
    GravitinoDorisCatalogSpark35 sparkCatalog =
        (GravitinoDorisCatalogSpark35)
            getSparkSession().sessionState().catalogManager().catalog(CATALOG_NAME);
    Identifier sparkIdentifier =
        Identifier.of(new String[] {DATABASE_NAME}, UNSUPPORTED_TABLE_NAME);
    try {
      sparkCatalog.invalidateTable(sparkIdentifier);
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> sparkCatalog.loadTable(sparkIdentifier));
    } finally {
      executeJdbcSql("DROP TABLE IF EXISTS " + dorisTable(UNSUPPORTED_TABLE_NAME));
    }
  }

  private static JDBCScan jdbcScan(Dataset<Row> result) {
    JDBCScan jdbcScan = null;
    Iterator<LogicalPlan> leaves =
        result.queryExecution().optimizedPlan().collectLeaves().iterator();
    while (leaves.hasNext()) {
      LogicalPlan leaf = leaves.next();
      if (leaf instanceof DataSourceV2ScanRelation) {
        Scan scan = ((DataSourceV2ScanRelation) leaf).scan();
        if (scan instanceof V1ScanWrapper) {
          scan = ((V1ScanWrapper) scan).v1Scan();
        }
        Assertions.assertTrue(scan instanceof JDBCScan, scan.getClass().getName());
        jdbcScan = (JDBCScan) scan;
      }
    }
    Assertions.assertNotNull(jdbcScan);
    return jdbcScan;
  }

  private void executeJdbcSql(String... statements) throws SQLException {
    try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        Statement statement = connection.createStatement()) {
      for (String sql : statements) {
        statement.execute(sql);
      }
    }
  }

  private static String dorisTable(String tableName) {
    return DATABASE_NAME + "." + tableName;
  }

  private static String sparkTable(String tableName) {
    return CATALOG_NAME + "." + dorisTable(tableName);
  }

  private static Stream<Arguments> ordinaryScalarTypes() {
    return Stream.of(
        Arguments.of("BIGINT", "9223372036854775806", 9223372036854775806L),
        Arguments.of("BOOLEAN", "TRUE", true),
        Arguments.of("DATE", "'2024-01-02'", Date.valueOf("2024-01-02")),
        Arguments.of("DATEV2", "'2024-01-02'", Date.valueOf("2024-01-02")),
        Arguments.of("DATETIME", "'2024-01-02 03:04:05'", Timestamp.valueOf("2024-01-02 03:04:05")),
        Arguments.of(
            "DATETIMEV2(0)", "'2024-01-02 03:04:05'", Timestamp.valueOf("2024-01-02 03:04:05")),
        Arguments.of("DECIMAL(10,2)", "12.34", new BigDecimal("12.34")),
        Arguments.of(
            "DECIMALV3(20,4)", "1234567890123456.1234", new BigDecimal("1234567890123456.1234")),
        Arguments.of("DOUBLE", "2.25", 2.25d),
        Arguments.of("FLOAT", "-123.125", -123.125f),
        Arguments.of("INT", "2147483647", Integer.MAX_VALUE),
        Arguments.of("INTEGER", "-2147483648", Integer.MIN_VALUE),
        Arguments.of("STRING", "'string-value'", "string-value"),
        Arguments.of("TEXT", "'text-value'", "text-value"),
        Arguments.of("TINYINT", "-128", (byte) -128),
        Arguments.of("VARCHAR(64)", "'varchar-value'", "varchar-value"));
  }
}
