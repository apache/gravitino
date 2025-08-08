/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.spark.connector.integration.test.authorization;

import static org.apache.gravitino.integration.test.util.TestDatabaseName.MYSQL_CATALOG_MYSQL_IT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConstants;
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class SparkAuthorizationIT extends BaseIT {

  protected static final String METALAKE = "metalake";

  protected static final String USER = "tester";

  protected static final String NORMAL_USER = "tester2";

  protected static final String JDBC_CATALOG = "jdbcCatalog";

  protected static final String JDBC_DATABASE = "jdbcDatabase";

  private String gravitinoUri;

  protected String mysqlUrl;

  protected String mysqlUsername;

  protected String mysqlPassword;

  protected String mysqlDriver;

  private SparkSession adminUserSparkSession;

  private SparkSession normalUserSparkSession;

  protected final String TIME_ZONE_UTC = "UTC";

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    // Enable authorization
    customConfigs.putAll(
        ImmutableMap.of(
            "SimpleAuthUserName",
            USER,
            Configs.ENABLE_AUTHORIZATION.getKey(),
            "true",
            Configs.CACHE_ENABLED.getKey(),
            "false",
            Configs.AUTHENTICATORS.getKey(),
            "simple"));
    putServiceAdmin();
    initMysqlContainer();
    super.startIntegrationTest();
    gravitinoUri = String.format("http://127.0.0.1:%d", getGravitinoServerPort());
    initMetalakeAndCatalogs();
    initSparkEnv();
  }

  private void initMysqlContainer() throws SQLException {
    containerSuite.startMySQLContainer(TestDatabaseName.MYSQL_CATALOG_MYSQL_IT);
    mysqlUrl = containerSuite.getMySQLContainer().getJdbcUrl();
    mysqlUsername = containerSuite.getMySQLContainer().getUsername();
    mysqlPassword = containerSuite.getMySQLContainer().getPassword();
    mysqlDriver = containerSuite.getMySQLContainer().getDriverClassName(MYSQL_CATALOG_MYSQL_IT);
  }

  private void initMetalakeAndCatalogs() {
    client.createMetalake(METALAKE, "", new HashMap<>());
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.addUser(NORMAL_USER);
    Map<String, String> properties = Maps.newHashMap();
    properties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_URL, mysqlUrl);
    properties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_USER, mysqlUsername);
    properties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD, mysqlPassword);
    properties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER, mysqlDriver);
    client
        .loadMetalake(METALAKE)
        .createCatalog(JDBC_CATALOG, Catalog.Type.RELATIONAL, "jdbc-mysql", "comment", properties);
  }

  protected void putServiceAdmin() {
    customConfigs.put(Configs.SERVICE_ADMINS.getKey(), USER);
  }

  private void initSparkEnv() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.plugins", GravitinoSparkPlugin.class.getName())
            .set(GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri)
            .set(GravitinoSparkConfig.GRAVITINO_METALAKE, METALAKE)
            .set("spark.sql.session.timeZone", TIME_ZONE_UTC);
    setEnv("HADOOP_USER_NAME", USER);
    adminUserSparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Spark connector integration test")
            .config(sparkConf)
            .getOrCreate();
    setEnv("HADOOP_USER_NAME", NORMAL_USER);
    normalUserSparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Spark connector integration test")
            .config(sparkConf)
            .enableHiveSupport()
            .getOrCreate();
  }

  @Test
  public void testCreateSchema() {
    List<Row> catalogs =
        executeSqlByUser(NORMAL_USER, () -> normalUserSparkSession.sql("show catalogs"))
            .collectAsList();
    assertListEquals(List.of(), catalogs);
    catalogs =
        executeSqlByUser(USER, () -> adminUserSparkSession.sql("show catalogs")).collectAsList();
    assertListEquals(List.of(new GenericRow(new String[] {JDBC_CATALOG})), catalogs);
    assertThrows(
        String.format(
            "%s is not authorized to perform operation 'loadMetalake' on metadata 'metalake'",
            NORMAL_USER),
        RuntimeException.class,
        () -> {
          executeSqlByUser(
              NORMAL_USER, () -> normalUserSparkSession.sql("create database " + JDBC_DATABASE));
        });
    executeSqlByUser(USER, () -> adminUserSparkSession.sql("create database " + JDBC_DATABASE));
  }

  private <E> void assertListEquals(List<E> expected, List<E> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }

  private <R> R executeSqlByUser(String user, Supplier<R> supplier) {
    setEnv("HADOOP_USER_NAME", user);
    return supplier.get();
  }
}
