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
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Spark 3.5 integration tests for the governed Doris read baseline. */
@Tag("gravitino-docker-test")
@Tag("doris-multi-version")
public class SparkJdbcDorisReadBaselineIT35 extends SparkEnvIT {

  private static final String CATALOG_NAME = "jdbc_doris";
  private static final String DATABASE_NAME = "doris_spark_baseline_it";
  private static final String TABLE_NAME = "read_baseline";
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
}
