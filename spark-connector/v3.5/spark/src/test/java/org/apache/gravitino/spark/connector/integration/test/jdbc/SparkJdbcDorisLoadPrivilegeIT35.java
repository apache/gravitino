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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Verifies that native Doris LOAD privilege remains mandatory for governed Spark writes. */
@Tag("gravitino-docker-test")
public class SparkJdbcDorisLoadPrivilegeIT35 extends SparkEnvIT {

  private static final String CATALOG_NAME = "jdbc_doris_load_denial";
  private static final String DATABASE_NAME = "doris_spark_load_denial_it";
  private static final String TABLE_NAME = "write_denied";
  private static final String NO_DROP_CATALOG_NAME = "jdbc_doris_no_drop";
  private static final String NO_DROP_TABLE_NAME = "write_no_drop";
  private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
  private static final String READ_ONLY_USER = "gravitino_spark_read_only";
  private static final String NO_DROP_USER = "gravitino_spark_no_drop";
  private static final DorisImageName DORIS_IMAGE = dorisImage();

  private String jdbcUrl;
  private String readOnlyPassword;
  private String noDropPassword;
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
    properties.put(GRAVITINO_JDBC_USER, READ_ONLY_USER);
    properties.put(GRAVITINO_JDBC_PASSWORD, readOnlyPassword);
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
    readOnlyPassword = "it-" + UUID.randomUUID().toString().replace("-", "");
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
      statement.execute("DROP TABLE IF EXISTS " + DATABASE_NAME + "." + NO_DROP_TABLE_NAME);
      statement.execute(
          "CREATE TABLE "
              + DATABASE_NAME
              + "."
              + NO_DROP_TABLE_NAME
              + " (id INT, name VARCHAR(64)) DISTRIBUTED BY HASH(id) BUCKETS 1");
      statement.execute(
          "INSERT INTO " + DATABASE_NAME + "." + TABLE_NAME + " VALUES (1, 'preserved')");
      statement.execute(
          "INSERT INTO " + DATABASE_NAME + "." + NO_DROP_TABLE_NAME + " VALUES (1, 'preserved')");
      statement.execute("DROP USER IF EXISTS '" + READ_ONLY_USER + "'");
      statement.execute(
          "CREATE USER '" + READ_ONLY_USER + "' IDENTIFIED BY '" + readOnlyPassword + "'");
      statement.execute(
          "GRANT SELECT_PRIV ON `" + DATABASE_NAME + "`.* TO '" + READ_ONLY_USER + "'");
      noDropPassword = "it-" + UUID.randomUUID().toString().replace("-", "");
      statement.execute("DROP USER IF EXISTS '" + NO_DROP_USER + "'");
      statement.execute(
          "CREATE USER '" + NO_DROP_USER + "' IDENTIFIED BY '" + noDropPassword + "'");
      statement.execute(
          "GRANT SELECT_PRIV, LOAD_PRIV ON `" + DATABASE_NAME + "`.* TO '" + NO_DROP_USER + "'");
    }
  }

  @Override
  protected void configureSparkConf(SparkConf sparkConf) {
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_ENABLE_DORIS_SUPPORT, "true");
  }

  @Test
  void testWriteFailsWithoutDorisLoadPrivilegeAndLeavesDataUnchanged() throws Exception {
    SparkException failure =
        Assertions.assertThrows(
            SparkException.class,
            () ->
                sql(
                    "INSERT INTO "
                        + CATALOG_NAME
                        + "."
                        + DATABASE_NAME
                        + "."
                        + TABLE_NAME
                        + " VALUES (2, 'denied')"));

    assertSecretNotExposed(failure, readOnlyPassword);
    Assertions.assertEquals(1, rowCount());
    Assertions.assertEquals(List.of(1), ids());
  }

  @Test
  void testLoadWithoutDropAllowsAppend() throws Exception {
    ensureNoDropCatalog();
    String table = NO_DROP_CATALOG_NAME + "." + DATABASE_NAME + "." + NO_DROP_TABLE_NAME;

    sql("INSERT INTO " + table + " VALUES (2, 'append-allowed')");
    await()
        .atMost(Duration.ofMinutes(1))
        .untilAsserted(() -> Assertions.assertEquals(List.of(1, 2), ids(NO_DROP_TABLE_NAME)));
  }

  private void ensureNoDropCatalog() {
    GravitinoMetalake metalake = client.loadMetalake("test");
    if (!metalake.catalogExists(NO_DROP_CATALOG_NAME)) {
      Map<String, String> properties = new HashMap<>(getCatalogConfigs());
      properties.put(GRAVITINO_JDBC_USER, NO_DROP_USER);
      properties.put(GRAVITINO_JDBC_PASSWORD, noDropPassword);
      metalake.createCatalog(
          NO_DROP_CATALOG_NAME,
          Catalog.Type.RELATIONAL,
          getProvider(),
          "Doris Spark account with LOAD but without DROP",
          properties);
    }
    getSparkSession()
        .conf()
        .set(
            "spark.sql.catalog." + NO_DROP_CATALOG_NAME,
            GravitinoDorisCatalogSpark35.class.getName());
  }

  private void assertSecretNotExposed(Throwable failure, String secret) {
    Throwable current = failure;
    while (current != null) {
      Assertions.assertFalse(String.valueOf(current.getMessage()).contains(secret));
      current = current.getCause();
    }
  }

  private int rowCount() throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                jdbcUrl, DorisContainer.USER_NAME, DorisContainer.PASSWORD);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT COUNT(*) FROM " + DATABASE_NAME + "." + TABLE_NAME)) {
      Assertions.assertTrue(resultSet.next());
      return resultSet.getInt(1);
    }
  }

  private List<Integer> ids() throws Exception {
    return ids(TABLE_NAME);
  }

  private List<Integer> ids(String tableName) throws Exception {
    List<Integer> ids = new ArrayList<>();
    try (Connection connection =
            DriverManager.getConnection(
                jdbcUrl, DorisContainer.USER_NAME, DorisContainer.PASSWORD);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT id FROM " + DATABASE_NAME + "." + tableName + " ORDER BY id")) {
      while (resultSet.next()) {
        ids.add(resultSet.getInt(1));
      }
    }
    return ids;
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
