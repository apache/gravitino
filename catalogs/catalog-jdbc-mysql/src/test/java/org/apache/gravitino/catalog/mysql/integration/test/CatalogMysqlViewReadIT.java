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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.mysql.integration.test.service.MysqlService;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/** Integration tests for MySQL view read operations (list/load). */
@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
public class CatalogMysqlViewReadIT extends BaseIT {

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static final String provider = "jdbc-mysql";

  private final String metalakeName = GravitinoITUtils.genRandomName("mysql_view_it_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("mysql_view_it_catalog");
  private final String schemaName = GravitinoITUtils.genRandomName("mysql_view_it_schema");

  private GravitinoMetalake metalake;
  private Catalog catalog;
  private MysqlService mysqlService;
  private MySQLContainer mysqlContainer;
  private TestDatabaseName testDbName;

  @BeforeAll
  public void startup() throws IOException, SQLException {
    testDbName = TestDatabaseName.MYSQL_CATALOG_MYSQL_IT;
    containerSuite.startMySQLContainer(testDbName);
    mysqlContainer = containerSuite.getMySQLContainer();
    mysqlService = new MysqlService(mysqlContainer, testDbName);

    createMetalake();
    catalog = createCatalog();
    createSchema();
    createBaseTableAndViews();
  }

  @AfterAll
  public void stop() {
    cleanupViewsAndTables();
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.disableCatalog(catalogName);
    metalake.dropCatalog(catalogName);
    client.disableMetalake(metalakeName);
    client.dropMetalake(metalakeName);
    mysqlService.close();
  }

  @AfterEach
  public void resetSchema() {
    cleanupViewsAndTables();
    catalog.asSchemas().dropSchema(schemaName, false);
    createSchema();
    createBaseTableAndViews();
  }

  @Test
  public void testListViews() {
    ViewCatalog viewCatalog = catalog.asViewCatalog();
    NameIdentifier[] views = viewCatalog.listViews(Namespace.of(schemaName));

    List<String> names =
        Arrays.stream(views).map(NameIdentifier::name).sorted().collect(Collectors.toList());
    Assertions.assertEquals(Arrays.asList("test_view_1", "test_view_2"), names);
  }

  @Test
  public void testListViewsInEmptySchema() {
    String emptySchema = GravitinoITUtils.genRandomName("empty_schema");
    catalog.asSchemas().createSchema(emptySchema, null, Collections.emptyMap());
    try {
      NameIdentifier[] views = catalog.asViewCatalog().listViews(Namespace.of(emptySchema));
      Assertions.assertEquals(0, views.length);
    } finally {
      catalog.asSchemas().dropSchema(emptySchema, false);
    }
  }

  @Test
  public void testListViewsInNonExistentSchema() {
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> catalog.asViewCatalog().listViews(Namespace.of("no_such_schema_xyz")));
  }

  @Test
  public void testLoadView() {
    View view =
        catalog
            .asViewCatalog()
            .loadView(NameIdentifier.of(Namespace.of(schemaName), "test_view_1"));

    Assertions.assertEquals("test_view_1", view.name());

    Column[] columns = view.columns();
    Assertions.assertEquals(2, columns.length);
    Assertions.assertEquals("id", columns[0].name());
    Assertions.assertNotNull(columns[0].dataType());
    Assertions.assertEquals("name", columns[1].name());
    Assertions.assertNotNull(columns[1].dataType());

    Assertions.assertEquals(1, view.representations().length);
    Assertions.assertInstanceOf(SQLRepresentation.class, view.representations()[0]);
    SQLRepresentation rep = (SQLRepresentation) view.representations()[0];
    Assertions.assertEquals("mysql", rep.dialect());
    Assertions.assertFalse(rep.sql().isEmpty());
  }

  @Test
  public void testLoadNonExistentView() {
    Assertions.assertThrows(
        NoSuchViewException.class,
        () ->
            catalog
                .asViewCatalog()
                .loadView(NameIdentifier.of(Namespace.of(schemaName), "no_such_view")));
  }

  @Test
  public void testViewExists() {
    Assertions.assertTrue(
        catalog
            .asViewCatalog()
            .viewExists(NameIdentifier.of(Namespace.of(schemaName), "test_view_1")));
  }

  @Test
  public void testViewDoesNotExist() {
    Assertions.assertFalse(
        catalog
            .asViewCatalog()
            .viewExists(NameIdentifier.of(Namespace.of(schemaName), "no_such_view")));
  }

  private void createMetalake() {
    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    metalake = client.loadMetalake(metalakeName);
  }

  private Catalog createCatalog() throws SQLException {
    Map<String, String> catalogProperties = Maps.newHashMap();
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

    return metalake.createCatalog(
        catalogName, Catalog.Type.RELATIONAL, provider, "comment", catalogProperties);
  }

  private void createSchema() {
    catalog.asSchemas().createSchema(schemaName, null, Collections.emptyMap());
  }

  private void createBaseTableAndViews() {
    mysqlService.executeQuery(
        "CREATE TABLE " + schemaName + ".base_table (id INT, name VARCHAR(255), score DOUBLE)");
    mysqlService.executeQuery(
        "CREATE VIEW "
            + schemaName
            + ".test_view_1 AS SELECT id, name FROM "
            + schemaName
            + ".base_table");
    mysqlService.executeQuery(
        "CREATE VIEW "
            + schemaName
            + ".test_view_2 AS SELECT id FROM "
            + schemaName
            + ".base_table WHERE id > 0");
  }

  private void cleanupViewsAndTables() {
    mysqlService.executeQuery("DROP VIEW IF EXISTS " + schemaName + ".test_view_1");
    mysqlService.executeQuery("DROP VIEW IF EXISTS " + schemaName + ".test_view_2");
    mysqlService.executeQuery("DROP TABLE IF EXISTS " + schemaName + ".base_table");
  }
}
