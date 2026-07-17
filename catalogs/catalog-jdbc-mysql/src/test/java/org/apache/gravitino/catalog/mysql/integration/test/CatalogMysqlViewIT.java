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
package org.apache.gravitino.catalog.mysql.integration.test;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.mysql.integration.test.service.MysqlService;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.rel.Dialects;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogMysqlViewIT extends BaseIT {

  private static final ContainerSuite CONTAINER_SUITE = ContainerSuite.getInstance();
  private static final String PROVIDER = "jdbc-mysql";

  private final String metalakeName = GravitinoITUtils.genRandomName("mysql_view_it_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("mysql_view_it_catalog");
  private final String schemaName = GravitinoITUtils.genRandomName("mysql_view_it_schema");

  private GravitinoMetalake metalake;
  private Catalog catalog;
  private MysqlService mysqlService;
  private MySQLContainer mysqlContainer;

  @BeforeAll
  public void startup() throws IOException, SQLException {
    TestDatabaseName testDbName = TestDatabaseName.MYSQL_CATALOG_MYSQL_IT;
    CONTAINER_SUITE.startMySQLContainer(testDbName);
    mysqlContainer = CONTAINER_SUITE.getMySQLContainer();
    mysqlService = new MysqlService(mysqlContainer, testDbName);

    createMetalake();
    catalog = createCatalog(catalogName);
    createSchema(catalog, schemaName);
  }

  @AfterAll
  public void stop() {
    if (mysqlService != null) {
      mysqlService.close();
    }
    metalake.disableCatalog(catalogName);
    metalake.dropCatalog(catalogName);
    client.disableMetalake(metalakeName);
    client.dropMetalake(metalakeName);
  }

  @Test
  public void testListAndLoadView() {
    String tableName = GravitinoITUtils.genRandomName("base_table");
    String viewName = GravitinoITUtils.genRandomName("test_view");
    Namespace namespace = Namespace.of(schemaName);

    mysqlService.executeQuery(
        String.format(
            "CREATE TABLE `%s`.`%s` (id INT PRIMARY KEY, name VARCHAR(32))",
            schemaName, tableName));
    mysqlService.executeQuery(
        String.format(
            "CREATE VIEW `%s`.`%s` AS SELECT id, name FROM `%s`.`%s`",
            schemaName, viewName, schemaName, tableName));

    ViewCatalog viewCatalog = catalog.asViewCatalog();
    NameIdentifier viewIdent = NameIdentifier.of(namespace, viewName);

    NameIdentifier[] views = viewCatalog.listViews(namespace);
    Assertions.assertTrue(Arrays.stream(views).anyMatch(ident -> ident.name().equals(viewName)));

    NameIdentifier[] tables = catalog.asTableCatalog().listTables(namespace);
    Assertions.assertTrue(Arrays.stream(tables).noneMatch(ident -> ident.name().equals(viewName)));

    View loadedView = viewCatalog.loadView(viewIdent);
    Assertions.assertEquals(viewName, loadedView.name());
    Assertions.assertEquals(2, loadedView.columns().length);
    Optional<SQLRepresentation> sqlRepresentation = loadedView.sqlFor(Dialects.MYSQL);
    Assertions.assertTrue(sqlRepresentation.isPresent());
    Assertions.assertTrue(
        sqlRepresentation.get().sql().toLowerCase().contains("select"),
        sqlRepresentation.get().sql());

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            viewCatalog.createView(
                NameIdentifier.of(namespace, GravitinoITUtils.genRandomName("new_view")),
                null,
                loadedView.columns(),
                loadedView.representations(),
                null,
                null,
                null));

    Assertions.assertThrows(
        NoSuchViewException.class,
        () -> viewCatalog.loadView(NameIdentifier.of(namespace, "missing_view")));
  }

  private void createMetalake() {
    GravitinoMetalake createdMetalake = client.createMetalake(metalakeName, "comment", null);
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(createdMetalake.name(), loadMetalake.name());
    metalake = loadMetalake;
  }

  private Catalog createCatalog(String catalogName) throws SQLException {
    Map<String, String> catalogProperties = Maps.newHashMap();
    TestDatabaseName testDbName = TestDatabaseName.MYSQL_CATALOG_MYSQL_IT;

    catalogProperties.put(
        JdbcConfig.JDBC_URL.getKey(),
        StringUtils.substring(
                mysqlContainer.getJdbcUrl(testDbName),
                0,
                mysqlContainer.getJdbcUrl(testDbName).lastIndexOf("/"))
            + "?useSSL=false&allowPublicKeyRetrieval=true");
    catalogProperties.put(
        JdbcConfig.JDBC_DRIVER.getKey(), mysqlContainer.getDriverClassName(testDbName));
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), mysqlContainer.getUsername());
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), mysqlContainer.getPassword());

    Catalog createdCatalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, PROVIDER, "comment", catalogProperties);
    Catalog loadCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(createdCatalog, loadCatalog);
    return loadCatalog;
  }

  private void createSchema(Catalog catalog, String schemaName) {
    Schema createdSchema = catalog.asSchemas().createSchema(schemaName, null, Maps.newHashMap());
    Schema loadSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());
  }
}
