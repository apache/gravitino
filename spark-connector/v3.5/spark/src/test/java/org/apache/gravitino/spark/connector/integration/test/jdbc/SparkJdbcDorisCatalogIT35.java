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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.DorisContainer;
import org.apache.gravitino.integration.test.container.DorisImageName;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.integration.test.SparkEnvIT;
import org.apache.gravitino.spark.connector.jdbc.doris.GravitinoDorisCatalogSpark35;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Spark 3.5 integration tests for the Gravitino-owned Doris batch-read adapter. */
@Tag("gravitino-docker-test")
public class SparkJdbcDorisCatalogIT35 extends SparkEnvIT {

  private static final String DORIS_SPARK_CONNECTOR_JAR = "gravitino.doris.spark.connector.jar";
  private static final String CATALOG_NAME = "jdbc_doris";
  private static final String DATABASE_NAME = "doris_spark_it";
  private static final String TABLE_NAME = "read_smoke";
  private static final String SPECIAL_TABLE_NAME = "read_special";
  private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
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
    feHttpPort = container.getFeHttpPort();
    try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
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
    }
  }

  @Override
  protected void configureSparkConf(SparkConf sparkConf) {
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_ENABLE_DORIS_SUPPORT, "true");
    if ("deploy".equals(System.getProperty("testMode"))) {
      String dorisConnectorJar = System.getProperty(DORIS_SPARK_CONNECTOR_JAR);
      Assertions.assertNotNull(
          dorisConnectorJar,
          "Deploy-mode Doris integration tests require an external Doris Spark Connector JAR");
      sparkConf.set("spark.jars", dorisConnectorJar);
    }
  }

  @Test
  void testCatalogClassName() {
    String className =
        getSparkSession().sessionState().conf().getConfString("spark.sql.catalog." + CATALOG_NAME);
    Assertions.assertEquals(GravitinoDorisCatalogSpark35.class.getName(), className);
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
    org.junit.jupiter.api.Assumptions.assumeTrue("deploy".equals(System.getProperty("testMode")));
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
