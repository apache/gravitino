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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.DorisContainer;
import org.apache.gravitino.integration.test.container.DorisImageName;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.integration.test.SparkEnvIT;

/** Shared Doris fixture for native reader integration tests. */
abstract class DorisNativeReadFixture35 extends SparkEnvIT {
  protected static final String CATALOG_NAME = "jdbc_doris";
  protected static final String TABLE_NAME = "native_read";
  private static final DorisImageName DORIS_IMAGE = DorisImageName.VERSION_3_0;
  protected final String databaseName =
      "spark_native_" + UUID.randomUUID().toString().replace("-", "");
  protected String jdbcUrl;
  private String feHttpAddress;
  private int feFlightPort;

  /**
   * Returns the explicitly selected Doris reader mode for the integration suite.
   *
   * @return the reader mode
   */
  protected String readMode() {
    return System.getenv().getOrDefault("GRAVITINO_TEST_DORIS_READ_MODE", "thrift");
  }

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
    suite.startDorisContainer(DORIS_IMAGE);
    DorisContainer container = suite.getDorisContainer(DORIS_IMAGE);
    jdbcUrl =
        String.format(
            "jdbc:mysql://%s:%d/", container.getContainerIpAddress(), container.getFeMysqlPort());
    feHttpAddress = container.getFeHttpHost() + ":" + container.getFeHttpPort();
    try (Connection connection =
            DriverManager.getConnection(
                jdbcUrl, DorisContainer.USER_NAME, DorisContainer.PASSWORD);
        Statement statement = connection.createStatement()) {
      if ("arrow".equalsIgnoreCase(readMode())) {
        try (ResultSet frontends = statement.executeQuery("SHOW FRONTENDS")) {
          if (!frontends.next()) {
            throw new IllegalStateException("Doris has no frontend for Arrow Flight SQL");
          }
          feHttpAddress = frontends.getString("Host") + ":" + frontends.getInt("HttpPort");
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
              + "."
              + TABLE_NAME
              + " (id INT NOT NULL, name VARCHAR(64)) DISTRIBUTED BY HASH(id) BUCKETS 8");
      statement.execute(
          "INSERT INTO "
              + databaseName
              + "."
              + TABLE_NAME
              + " VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'),"
              + " (5, 'five'), (6, 'six'), (7, 'seven'), (8, 'eight')");
    }
  }

  @Override
  protected Map<String, String> getExtraSparkConfigs() {
    return Map.of(GravitinoSparkConfig.GRAVITINO_ENABLE_DORIS_SUPPORT, "true");
  }

  protected String sparkTable() {
    return CATALOG_NAME + "." + databaseName + "." + TABLE_NAME;
  }
}
