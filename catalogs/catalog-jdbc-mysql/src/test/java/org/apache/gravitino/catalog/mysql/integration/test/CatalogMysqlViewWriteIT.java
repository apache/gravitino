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
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Dialects;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.rel.ViewChange;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/** Integration tests for MySQL view write operations (create/alter/drop). */
@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
public class CatalogMysqlViewWriteIT extends BaseIT {

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static final String provider = "jdbc-mysql";

  private final String metalakeName =
      GravitinoITUtils.genRandomName("mysql_view_write_it_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("mysql_view_write_it_catalog");
  private final String schemaName = GravitinoITUtils.genRandomName("mysql_view_write_it_schema");

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
    createBaseTable();
  }

  @AfterAll
  public void stop() {
    cleanupAll();
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.disableCatalog(catalogName);
    metalake.dropCatalog(catalogName);
    client.disableMetalake(metalakeName);
    client.dropMetalake(metalakeName);
    mysqlService.close();
  }

  @AfterEach
  public void resetSchema() {
    cleanupAll();
    catalog.asSchemas().dropSchema(schemaName, true);
    createSchema();
    createBaseTable();
  }

  @Test
  public void testCreateViewAndLoad() {
    ViewCatalog viewCatalog = catalog.asViewCatalog();
    NameIdentifier ident = NameIdentifier.of(Namespace.of(schemaName), "created_view");
    SQLRepresentation rep =
        SQLRepresentation.builder()
            .withDialect(Dialects.MYSQL)
            .withSql("SELECT id, name FROM base_table")
            .build();

    View created =
        viewCatalog.createView(
            ident, "test comment", new Column[0], new Representation[] {rep}, null, null, null);

    Assertions.assertEquals("created_view", created.name());

    View loaded = viewCatalog.loadView(ident);
    Assertions.assertEquals("created_view", loaded.name());
    Assertions.assertTrue(loaded.columns().length >= 2);
  }

  @Test
  public void testCreateViewAlreadyExistsThrows() {
    ViewCatalog viewCatalog = catalog.asViewCatalog();
    NameIdentifier ident = NameIdentifier.of(Namespace.of(schemaName), "dup_view");
    SQLRepresentation rep =
        SQLRepresentation.builder().withDialect(Dialects.MYSQL).withSql("SELECT 1").build();

    viewCatalog.createView(
        ident, null, new Column[0], new Representation[] {rep}, null, null, null);

    Assertions.assertThrows(
        ViewAlreadyExistsException.class,
        () ->
            viewCatalog.createView(
                ident, null, new Column[0], new Representation[] {rep}, null, null, null));
  }

  @Test
  public void testCreateViewInNonExistentSchemaThrows() {
    ViewCatalog viewCatalog = catalog.asViewCatalog();
    NameIdentifier ident = NameIdentifier.of(Namespace.of("no_such_schema"), "v1");
    SQLRepresentation rep =
        SQLRepresentation.builder().withDialect(Dialects.MYSQL).withSql("SELECT 1").build();

    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () ->
            viewCatalog.createView(
                ident, null, new Column[0], new Representation[] {rep}, null, null, null));
  }

  @Test
  public void testAlterViewRename() {
    ViewCatalog viewCatalog = catalog.asViewCatalog();
    NameIdentifier ident = NameIdentifier.of(Namespace.of(schemaName), "rename_me");
    SQLRepresentation rep =
        SQLRepresentation.builder()
            .withDialect(Dialects.MYSQL)
            .withSql("SELECT id FROM base_table")
            .build();
    viewCatalog.createView(
        ident, null, new Column[0], new Representation[] {rep}, null, null, null);

    View renamed = viewCatalog.alterView(ident, ViewChange.rename("renamed_view"));
    Assertions.assertEquals("renamed_view", renamed.name());
    Assertions.assertTrue(
        viewCatalog.viewExists(NameIdentifier.of(Namespace.of(schemaName), "renamed_view")));
    Assertions.assertFalse(viewCatalog.viewExists(ident));
  }

  @Test
  public void testAlterViewReplace() {
    ViewCatalog viewCatalog = catalog.asViewCatalog();
    NameIdentifier ident = NameIdentifier.of(Namespace.of(schemaName), "replace_me");
    SQLRepresentation rep =
        SQLRepresentation.builder()
            .withDialect(Dialects.MYSQL)
            .withSql("SELECT id FROM base_table")
            .build();
    viewCatalog.createView(
        ident, null, new Column[0], new Representation[] {rep}, null, null, null);

    SQLRepresentation newRep =
        SQLRepresentation.builder()
            .withDialect(Dialects.MYSQL)
            .withSql("SELECT id, name FROM base_table")
            .build();
    viewCatalog.alterView(
        ident,
        ViewChange.replaceView(new Column[0], new Representation[] {newRep}, null, null, null));

    View loaded = viewCatalog.loadView(ident);
    Assertions.assertTrue(loaded.columns().length >= 2, "Replaced view should have 2+ columns");
  }

  @Test
  public void testDropView() {
    ViewCatalog viewCatalog = catalog.asViewCatalog();
    NameIdentifier ident = NameIdentifier.of(Namespace.of(schemaName), "drop_me");
    SQLRepresentation rep =
        SQLRepresentation.builder().withDialect(Dialects.MYSQL).withSql("SELECT 1").build();
    viewCatalog.createView(
        ident, null, new Column[0], new Representation[] {rep}, null, null, null);

    Assertions.assertTrue(viewCatalog.dropView(ident));
    Assertions.assertFalse(viewCatalog.viewExists(ident));
  }

  @Test
  public void testDropNonExistentViewReturnsFalse() {
    Assertions.assertFalse(
        catalog
            .asViewCatalog()
            .dropView(NameIdentifier.of(Namespace.of(schemaName), "no_such_view")));
  }

  @Test
  public void testListViewsAfterCreateAndDrop() {
    ViewCatalog viewCatalog = catalog.asViewCatalog();
    SQLRepresentation rep =
        SQLRepresentation.builder().withDialect(Dialects.MYSQL).withSql("SELECT 1").build();

    viewCatalog.createView(
        NameIdentifier.of(Namespace.of(schemaName), "list_v1"),
        null,
        new Column[0],
        new Representation[] {rep},
        null,
        null,
        null);
    viewCatalog.createView(
        NameIdentifier.of(Namespace.of(schemaName), "list_v2"),
        null,
        new Column[0],
        new Representation[] {rep},
        null,
        null,
        null);
    viewCatalog.createView(
        NameIdentifier.of(Namespace.of(schemaName), "list_v3"),
        null,
        new Column[0],
        new Representation[] {rep},
        null,
        null,
        null);

    List<String> names =
        Arrays.stream(viewCatalog.listViews(Namespace.of(schemaName)))
            .map(NameIdentifier::name)
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(Arrays.asList("list_v1", "list_v2", "list_v3"), names);

    viewCatalog.dropView(NameIdentifier.of(Namespace.of(schemaName), "list_v2"));

    List<String> afterDrop =
        Arrays.stream(viewCatalog.listViews(Namespace.of(schemaName)))
            .map(NameIdentifier::name)
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(Arrays.asList("list_v1", "list_v3"), afterDrop);
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

  private void createBaseTable() {
    mysqlService.executeQuery(
        "CREATE TABLE " + schemaName + ".base_table (id INT, name VARCHAR(255))");
  }

  private void cleanupAll() {
    NameIdentifier[] views = catalog.asViewCatalog().listViews(Namespace.of(schemaName));
    for (NameIdentifier view : views) {
      catalog.asViewCatalog().dropView(view);
    }
  }
}
