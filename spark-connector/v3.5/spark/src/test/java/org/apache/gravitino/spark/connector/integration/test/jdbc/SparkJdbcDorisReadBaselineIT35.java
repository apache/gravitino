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
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
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
  private static final String UNSUPPORTED_TABLE_NAME = "unsupported_json";
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
    try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME);
      statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + TABLE_NAME);
      statement.execute(
          "CREATE TABLE "
              + DATABASE_NAME
              + "."
              + TABLE_NAME
              + " (id INT NOT NULL, name VARCHAR(64)) DISTRIBUTED BY HASH(id) BUCKETS 1");
      statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + UNSUPPORTED_TABLE_NAME);
      statement.execute(
          "CREATE TABLE "
              + DATABASE_NAME
              + "."
              + UNSUPPORTED_TABLE_NAME
              + " (id INT NOT NULL, payload JSON) DISTRIBUTED BY HASH(id) BUCKETS 1");
      statement.execute(
          "INSERT INTO " + DATABASE_NAME + "." + TABLE_NAME + " VALUES (1, 'one'), (2, 'two')");
    }
  }

  @Override
  protected Map<String, String> getExtraSparkConfigs() {
    return Collections.singletonMap(GravitinoSparkConfig.GRAVITINO_ENABLE_DORIS_SUPPORT, "true");
  }

  @Test
  void testCatalogClassName() {
    String className =
        getSparkSession().sessionState().conf().getConfString("spark.sql.catalog." + CATALOG_NAME);
    Assertions.assertEquals(GravitinoDorisCatalogSpark35.class.getName(), className);
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
  void testCatalogBypassReadOptionControlsPredicatePushdown() {
    Dataset<Row> result =
        getSparkSession()
            .sql(
                "SELECT id, name FROM "
                    + CATALOG_NAME
                    + "."
                    + DATABASE_NAME
                    + "."
                    + TABLE_NAME
                    + " WHERE id = 1");
    List<Row> rows = result.collectAsList();

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(1, rows.get(0).getInt(0));
    Assertions.assertEquals("one", rows.get(0).getString(1));
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
        statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + tableName);
        statement.execute(
            "CREATE TABLE "
                + DATABASE_NAME
                + "."
                + tableName
                + " (id INT NOT NULL, required_value "
                + sqlType
                + " NOT NULL, nullable_value "
                + sqlType
                + " NULL) DISTRIBUTED BY HASH(id) BUCKETS 1");
        statement.execute(
            "INSERT INTO "
                + DATABASE_NAME
                + "."
                + tableName
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
                  "SELECT required_value FROM "
                      + DATABASE_NAME
                      + "."
                      + tableName
                      + " WHERE id = 1")) {
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
                      + CATALOG_NAME
                      + "."
                      + DATABASE_NAME
                      + "."
                      + tableName
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
      try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
          Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + tableName);
      }
    }
  }

  @Test
  void testSmallIntMatchesGenericJdbcPath() throws Exception {
    String tableName = ORDINARY_TYPES_TABLE_NAME + "_smallint";
    try {
      try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
          Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + tableName);
        statement.execute(
            "CREATE TABLE "
                + DATABASE_NAME
                + "."
                + tableName
                + " (id INT NOT NULL, required_value SMALLINT NOT NULL, "
                + "nullable_value SMALLINT NULL) DISTRIBUTED BY HASH(id) BUCKETS 1");
        statement.execute(
            "INSERT INTO "
                + DATABASE_NAME
                + "."
                + tableName
                + " VALUES (1, -32768, -32768), (2, 32767, 32767), "
                + "(3, -32768, NULL), (4, 32767, NULL)");
      }

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
                    + CATALOG_NAME
                    + "."
                    + DATABASE_NAME
                    + "."
                    + tableName
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
      try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
          Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + tableName);
      }
    }
  }

  @Test
  void testReadIgnoresColumnsFromOtherTablesAndDatabases() throws Exception {
    try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS " + OTHER_DATABASE_NAME);
      statement.execute("DROP TABLE IF EXISTS " + OTHER_DATABASE_NAME + "." + TABLE_NAME);
      statement.execute(
          "CREATE TABLE "
              + OTHER_DATABASE_NAME
              + "."
              + TABLE_NAME
              + " (unrelated INT) DISTRIBUTED BY HASH(unrelated) BUCKETS 1");
      statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + PATTERN_MATCH_TABLE_NAME);
      statement.execute(
          "CREATE TABLE "
              + DATABASE_NAME
              + "."
              + PATTERN_MATCH_TABLE_NAME
              + " (unrelated INT) DISTRIBUTED BY HASH(unrelated) BUCKETS 1");

      GravitinoDorisCatalogSpark35 sparkCatalog =
          (GravitinoDorisCatalogSpark35)
              getSparkSession().sessionState().catalogManager().catalog(CATALOG_NAME);
      sparkCatalog.invalidateTable(Identifier.of(new String[] {DATABASE_NAME}, TABLE_NAME));
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
    } finally {
      try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
          Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + PATTERN_MATCH_TABLE_NAME);
        statement.execute("DROP TABLE IF EXISTS " + OTHER_DATABASE_NAME + "." + TABLE_NAME);
        statement.execute("DROP DATABASE IF EXISTS " + OTHER_DATABASE_NAME);
      }
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
                    java.util.Collections.emptySet()));
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
      try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
          Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + UNSUPPORTED_TABLE_NAME);
      }
    }
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
        Arguments.of("INT", "2147483647", Integer.MAX_VALUE),
        Arguments.of("INTEGER", "-2147483648", Integer.MIN_VALUE),
        Arguments.of("STRING", "'string-value'", "string-value"),
        Arguments.of("TEXT", "'text-value'", "text-value"),
        Arguments.of("TINYINT", "-128", (byte) -128),
        Arguments.of("VARCHAR(64)", "'varchar-value'", "varchar-value"));
  }
}
