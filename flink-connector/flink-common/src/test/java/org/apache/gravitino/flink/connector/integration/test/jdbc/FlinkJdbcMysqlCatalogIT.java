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

package org.apache.gravitino.flink.connector.integration.test.jdbc;

import static org.apache.gravitino.integration.test.util.TestDatabaseName.MYSQL_CATALOG_MYSQL_IT;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.flink.table.api.ValidationException;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.flink.connector.integration.test.FlinkCommonIT;
import org.apache.gravitino.flink.connector.jdbc.JdbcPropertiesConstants;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class FlinkJdbcMysqlCatalogIT extends FlinkCommonIT {

  protected String mysqlUrl;
  protected String mysqlUsername;
  protected String mysqlPassword;
  protected String mysqlDriver;
  protected String mysqlDefaultDatabase = MYSQL_CATALOG_MYSQL_IT.name();

  protected Catalog catalog;

  protected static final String CATALOG_NAME = "test_flink_jdbc_catalog";

  private static final String FLINK_BYPASS_DEFAULT_DATABASE = "flink.bypass.default-database";

  @Override
  protected boolean supportTablePropertiesOperation() {
    return false;
  }

  @Override
  protected String defaultDatabaseName() {
    return MYSQL_CATALOG_MYSQL_IT.name();
  }

  @Override
  protected boolean supportSchemaOperationWithCommentAndOptions() {
    return false;
  }

  @Override
  protected Catalog currentCatalog() {
    return catalog;
  }

  @Override
  protected String getProvider() {
    return "jdbc-mysql";
  }

  @BeforeAll
  void jdbcStartup() {
    init();
  }

  @AfterAll
  void jdbcStop() {
    Preconditions.checkArgument(metalake != null, "metalake is null");
    metalake.dropCatalog(CATALOG_NAME, true);
  }

  @Override
  protected boolean supportDropCascade() {
    return true;
  }

  @Override
  protected boolean defaultValueWithNullLiterals() {
    return true;
  }

  private void init() {
    Preconditions.checkArgument(metalake != null, "metalake is null");
    catalog =
        metalake.createCatalog(
            CATALOG_NAME,
            org.apache.gravitino.Catalog.Type.RELATIONAL,
            getProvider(),
            null,
            ImmutableMap.of(
                JdbcPropertiesConstants.GRAVITINO_JDBC_USER,
                mysqlUsername,
                JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD,
                mysqlPassword,
                JdbcPropertiesConstants.GRAVITINO_JDBC_URL,
                mysqlUrl,
                JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER,
                mysqlDriver,
                FLINK_BYPASS_DEFAULT_DATABASE,
                mysqlDefaultDatabase));
  }

  @Override
  protected void initCatalogEnv() throws Exception {
    ContainerSuite containerSuite = ContainerSuite.getInstance();
    containerSuite.startMySQLContainer(MYSQL_CATALOG_MYSQL_IT);
    mysqlUrl = containerSuite.getMySQLContainer().getJdbcUrl();
    mysqlUsername = containerSuite.getMySQLContainer().getUsername();
    mysqlPassword = containerSuite.getMySQLContainer().getPassword();
    mysqlDriver = containerSuite.getMySQLContainer().getDriverClassName(MYSQL_CATALOG_MYSQL_IT);
  }

  @Override
  protected void stopCatalogEnv() throws Exception {
    if (null != containerSuite) {
      containerSuite.close();
    }
  }

  @Test
  public void testCreateGravitinoJdbcCatalogUsingSQL() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;
    String catalogName = "gravitino_mysql_jdbc_catalog";
    tableEnv.executeSql(
        String.format(
            "create catalog %s with ("
                + "'type'='gravitino-jdbc-mysql', "
                + "'base-url'='%s',"
                + "'username'='%s',"
                + "'password'='%s',"
                + "'default-database'='%s'"
                + ")",
            catalogName, mysqlUrl, mysqlUsername, mysqlPassword, mysqlDefaultDatabase));
    String[] catalogs = tableEnv.listCatalogs();
    Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    org.apache.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals(mysqlUrl, properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_URL));
    Assertions.assertEquals(
        mysqlUsername, properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_USER));
    Assertions.assertEquals(
        mysqlPassword, properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_USER));
    Assertions.assertEquals(mysqlDefaultDatabase, properties.get(FLINK_BYPASS_DEFAULT_DATABASE));
    Assertions.assertEquals(
        "com.mysql.jdbc.Driver", properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER));
  }

  @Test
  public void testCreateGravitinoJdbcCatalogUsingSQLWithDriver() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;
    String catalogName = "gravitino_mysql_jdbc_catalog_with_driver";
    String driver = "com.mysql.cj.jdbc.Driver";
    tableEnv.executeSql(
        String.format(
            "create catalog %s with ("
                + "'type'='gravitino-jdbc-mysql', "
                + "'base-url'='%s',"
                + "'username'='%s',"
                + "'password'='%s',"
                + "'driver'='%s',"
                + "'default-database'='%s'"
                + ")",
            catalogName, mysqlUrl, mysqlUsername, mysqlPassword, driver, mysqlDefaultDatabase));
    String[] catalogs = tableEnv.listCatalogs();
    Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    org.apache.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals(driver, properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER));
  }

  @Test
  public void testCreateGravitinoJdbcCatalogUsingSQLMissingOptions() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    String catalogName = "gravitino_mysql_jdbc_catalog_missing_options";
    Assertions.assertThrows(
        ValidationException.class,
        () ->
            tableEnv.executeSql(
                String.format(
                    "create catalog %s with ("
                        + "'type'='gravitino-jdbc-mysql', "
                        + "'base-url'='%s',"
                        + "'username'='%s',"
                        + "'default-database'='%s'"
                        + ")",
                    catalogName, mysqlUrl, mysqlUsername, mysqlDefaultDatabase)),
        "Missing required options are:\n" + "\n" + "password");
  }
}
