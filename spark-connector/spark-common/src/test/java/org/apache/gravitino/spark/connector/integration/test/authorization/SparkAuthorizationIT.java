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
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConstants;
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class SparkAuthorizationIT extends BaseIT {

  private static final String METALAKE = "metalake";

  private static final String ADMIN_USER = "tester";

  private static final String NORMAL_USER = "tester2";

  private static final String JDBC_CATALOG = "jdbcCatalog";

  private static final String JDBC_DATABASE = "jdbcDatabase";

  private static final String ROLE = "role";

  private String gravitinoUri;

  protected String mysqlUrl;

  protected String mysqlUsername;

  protected String mysqlPassword;

  protected String mysqlDriver;

  private SparkSession normalUserSparkSession;

  protected final String TIME_ZONE_UTC = "UTC";

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    // Enable authorization
    customConfigs.putAll(
        ImmutableMap.of(
            "SimpleAuthUserName",
            ADMIN_USER,
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
    initTables();
    initSparkEnv();
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    containerSuite.close();
    setEnv("SPARK_USER", AuthConstants.ANONYMOUS_USER);
    // HADOOP_USER_NAME will be set by spark auto.
    setEnv("HADOOP_USER_NAME", AuthConstants.ANONYMOUS_USER);
    normalUserSparkSession.close();
    super.stopIntegrationTest();
  }

  private void initMysqlContainer() throws SQLException {
    containerSuite.startMySQLContainer(TestDatabaseName.MYSQL_CATALOG_MYSQL_IT);
    mysqlUrl = containerSuite.getMySQLContainer().getJdbcUrl();
    mysqlUsername = containerSuite.getMySQLContainer().getUsername();
    mysqlPassword = containerSuite.getMySQLContainer().getPassword();
    mysqlDriver = containerSuite.getMySQLContainer().getDriverClassName(MYSQL_CATALOG_MYSQL_IT);
  }

  private void initTables() {
    client.createMetalake(METALAKE, "", new HashMap<>());
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.addUser(NORMAL_USER);
    Map<String, String> properties = Maps.newHashMap();
    properties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_URL, mysqlUrl);
    properties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_USER, mysqlUsername);
    properties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD, mysqlPassword);
    properties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER, mysqlDriver);
    Catalog catalog =
        client
            .loadMetalake(METALAKE)
            .createCatalog(
                JDBC_CATALOG, Catalog.Type.RELATIONAL, "jdbc-mysql", "comment", properties);
    SupportsSchemas schemas = catalog.asSchemas();
    schemas.createSchema(JDBC_DATABASE, "", new HashMap<>());
    SecurableObject securableObject =
        SecurableObjects.ofCatalog(
            JDBC_CATALOG,
            ImmutableList.of(
                Privileges.UseCatalog.allow(),
                Privileges.UseSchema.allow(),
                Privileges.SelectTable.allow(),
                Privileges.CreateTable.allow()));
    gravitinoMetalake.createRole(ROLE, new HashMap<>(), ImmutableList.of(securableObject));
    gravitinoMetalake.grantRolesToUser(ImmutableList.of(ROLE), NORMAL_USER);
  }

  protected void putServiceAdmin() {
    customConfigs.put(Configs.SERVICE_ADMINS.getKey(), ADMIN_USER);
  }

  private void initSparkEnv() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.plugins", GravitinoSparkPlugin.class.getName())
            .set(GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri)
            .set(GravitinoSparkConfig.GRAVITINO_METALAKE, METALAKE)
            .set("spark.sql.session.timeZone", TIME_ZONE_UTC);
    setEnv("SPARK_USER", NORMAL_USER);
    normalUserSparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Spark connector integration test")
            .config(sparkConf)
            .getOrCreate();
  }

  @Test
  @Order(1)
  public void testCreateTable() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    normalUserSparkSession.sql("use " + JDBC_CATALOG);
    normalUserSparkSession.sql("use " + JDBC_DATABASE);
    List<Row> tables = normalUserSparkSession.sql("show tables").collectAsList();
    assertEqualsRows(new ArrayList<>(), tables);
    normalUserSparkSession.sql("CREATE TABLE table_a(a STRING)");
    TableCatalog tableCatalog = gravitinoMetalake.loadCatalog(JDBC_CATALOG).asTableCatalog();
    tableCatalog.createTable(
        NameIdentifier.of(JDBC_DATABASE, "table_b"),
        new Column[] {Column.of("col1", Types.StringType.get())},
        "",
        new HashMap<>());
    tableCatalog.createTable(
        NameIdentifier.of(JDBC_DATABASE, "table_c"),
        new Column[] {Column.of("col1", Types.StringType.get())},
        "",
        new HashMap<>());
    tables = normalUserSparkSession.sql("show tables").collectAsList();
    assertEqualsRows(
        ImmutableList.of(
            RowFactory.create("jdbcDatabase", "table_a", false),
            RowFactory.create("jdbcDatabase", "table_b", false),
            RowFactory.create("jdbcDatabase", "table_c", false)),
        tables);
  }

  @Test
  @Order(2)
  public void testListTable() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(
            ImmutableList.of(JDBC_CATALOG, JDBC_DATABASE), MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.CreateTable.deny()));
    assertThrows(
        "Can not access metadata {" + JDBC_CATALOG + "." + JDBC_DATABASE + "}.",
        ForbiddenException.class,
        () -> {
          normalUserSparkSession.sql("CREATE TABLE table_d(a STRING)");
        });
    List<Row> tables = normalUserSparkSession.sql("show tables").collectAsList();
    assertEqualsRows(
        ImmutableList.of(
            RowFactory.create("jdbcDatabase", "table_a", false),
            RowFactory.create("jdbcDatabase", "table_b", false),
            RowFactory.create("jdbcDatabase", "table_c", false)),
        tables);
  }

  @Test
  @Order(3)
  public void testAlterLoad() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(
            ImmutableList.of(JDBC_CATALOG, JDBC_DATABASE, "table_b"), MetadataObject.Type.TABLE),
        ImmutableList.of(Privileges.SelectTable.deny()));
    List<Row> tables = normalUserSparkSession.sql("show tables").collectAsList();
    assertEqualsRows(
        ImmutableList.of(
            RowFactory.create("jdbcDatabase", "table_a", false),
            RowFactory.create("jdbcDatabase", "table_c", false)),
        tables);
    assertThrows(
        "Can not access metadata {" + JDBC_CATALOG + "." + JDBC_DATABASE + "}.",
        ForbiddenException.class,
        () -> {
          normalUserSparkSession.sql("ALTER TABLE table_c ADD COLUMNS (b STRING);");
        });
  }

  @Test
  @Order(4)
  public void testDropTable() {
    assertThrows(
        "Can not access metadata {" + JDBC_CATALOG + "." + JDBC_DATABASE + "}.",
        ForbiddenException.class,
        () -> {
          normalUserSparkSession.sql("DROP TABLE table_c");
        });
    normalUserSparkSession.sql("DROP TABLE table_a");
  }

  private void assertEqualsRows(List<Row> exceptRows, List<Row> actualRows) {
    Assertions.assertEquals(exceptRows.size(), actualRows.size());
    for (int i = 0; i < exceptRows.size(); i++) {
      Row exceptRow = exceptRows.get(i);
      Row actualRow = actualRows.get(i);
      Assertions.assertEquals(exceptRow.length(), actualRow.length());
      for (int j = 0; j < exceptRow.length(); j++) {
        Assertions.assertEquals(exceptRow.get(j), actualRow.get(j));
      }
    }
  }
}
