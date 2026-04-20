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
package org.apache.gravitino.catalog.hive.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Container.ExecResult;

/** Integration tests for Hive catalog view CRUD operations. */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogHiveViewIT extends BaseIT {

  private static final String PROVIDER = "hive";
  private static final String VIEW_COMMENT = "hive view comment";
  private static final String HIVE_DIALECT = "hive";

  private final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private String metalakeName;
  private String catalogName;
  private String schemaName;

  private GravitinoMetalake metalake;
  private Catalog catalog;
  private ViewCatalog viewCatalog;

  @BeforeAll
  public void setup() throws Exception {
    super.startIntegrationTest();
    containerSuite.startHiveContainer(
        ImmutableMap.of(HiveContainer.HIVE_RUNTIME_VERSION, HiveContainer.HIVE2));

    metalakeName = GravitinoITUtils.genRandomName("hive_view_it_metalake");
    catalogName = GravitinoITUtils.genRandomName("hive_view_it_catalog");
    schemaName = GravitinoITUtils.genRandomName("hive_view_it_schema");

    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    metalake = client.loadMetalake(metalakeName);

    String hmsUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(HiveConstants.METASTORE_URIS, hmsUri);

    catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, PROVIDER, "comment", catalogProperties);
    viewCatalog = catalog.asViewCatalog();
    catalog.asSchemas().createSchema(schemaName, "schema comment", Maps.newHashMap());
  }

  @AfterEach
  public void cleanViews() {
    NameIdentifier[] views = viewCatalog.listViews(Namespace.of(schemaName));
    for (NameIdentifier view : views) {
      viewCatalog.dropView(view);
    }
  }

  @AfterAll
  public void teardown() throws Exception {
    try {
      catalog.asSchemas().dropSchema(schemaName, true);
      metalake.disableCatalog(catalogName);
      metalake.dropCatalog(catalogName, true);
      client.disableMetalake(metalakeName);
      client.dropMetalake(metalakeName);
    } finally {
      super.stopIntegrationTest();
    }
  }

  // Suppress parent-class lifecycle hooks to control lifecycle ourselves.
  @Override
  @BeforeAll
  public void startIntegrationTest() {}

  @Override
  @AfterAll
  public void stopIntegrationTest() {}

  @Test
  public void testCreateAndLoadView() {
    Column[] columns = {
      Column.of("id", Types.LongType.get(), "id column"),
      Column.of("name", Types.StringType.get(), "name column")
    };
    String viewName = GravitinoITUtils.genRandomName("hive_test_view");
    View created =
        viewCatalog.createView(
            NameIdentifier.of(schemaName, viewName),
            VIEW_COMMENT,
            columns,
            new SQLRepresentation[] {hiveRep("SELECT id, name FROM some_table")},
            null,
            null,
            Collections.singletonMap("created_by", "test"));

    Assertions.assertEquals(viewName, created.name());
    Assertions.assertEquals(VIEW_COMMENT, created.comment());
    Assertions.assertEquals(1, created.representations().length);
    Assertions.assertInstanceOf(SQLRepresentation.class, created.representations()[0]);
    Assertions.assertEquals(
        "SELECT id, name FROM some_table",
        ((SQLRepresentation) created.representations()[0]).sql());
    Assertions.assertEquals(
        HIVE_DIALECT, ((SQLRepresentation) created.representations()[0]).dialect());

    View loaded = viewCatalog.loadView(NameIdentifier.of(schemaName, viewName));
    Assertions.assertEquals(viewName, loaded.name());
    Assertions.assertEquals(VIEW_COMMENT, loaded.comment());
    Assertions.assertEquals(1, loaded.representations().length);
    Assertions.assertEquals("test", loaded.properties().get("created_by"));
  }

  @Test
  public void testListViews() {
    String view1 = GravitinoITUtils.genRandomName("hive_list_view1");
    String view2 = GravitinoITUtils.genRandomName("hive_list_view2");
    Column[] columns = {Column.of("c1", Types.StringType.get(), null)};

    viewCatalog.createView(
        NameIdentifier.of(schemaName, view1),
        null,
        columns,
        new SQLRepresentation[] {hiveRep("SELECT c1 FROM t")},
        null,
        null,
        Collections.emptyMap());
    viewCatalog.createView(
        NameIdentifier.of(schemaName, view2),
        null,
        columns,
        new SQLRepresentation[] {hiveRep("SELECT c1 FROM t")},
        null,
        null,
        Collections.emptyMap());

    NameIdentifier[] views = viewCatalog.listViews(Namespace.of(schemaName));
    Assertions.assertTrue(views.length >= 2);
    boolean foundView1 = false, foundView2 = false;
    for (NameIdentifier v : views) {
      if (v.name().equals(view1)) foundView1 = true;
      if (v.name().equals(view2)) foundView2 = true;
    }
    Assertions.assertTrue(foundView1, "view1 not found in list");
    Assertions.assertTrue(foundView2, "view2 not found in list");
  }

  @Test
  public void testListViewsInNonExistentSchema() {
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> viewCatalog.listViews(Namespace.of("non_existent_schema_xyz")));
  }

  @Test
  public void testViewExists() {
    String viewName = GravitinoITUtils.genRandomName("hive_exists_view");
    NameIdentifier ident = NameIdentifier.of(schemaName, viewName);
    Column[] columns = {Column.of("id", Types.LongType.get(), null)};

    Assertions.assertFalse(viewCatalog.viewExists(ident));
    viewCatalog.createView(
        ident,
        null,
        columns,
        new SQLRepresentation[] {hiveRep("SELECT id FROM t")},
        null,
        null,
        Collections.emptyMap());
    Assertions.assertTrue(viewCatalog.viewExists(ident));
  }

  @Test
  public void testDropView() {
    String viewName = GravitinoITUtils.genRandomName("hive_drop_view");
    NameIdentifier ident = NameIdentifier.of(schemaName, viewName);
    Column[] columns = {Column.of("id", Types.LongType.get(), null)};

    viewCatalog.createView(
        ident,
        null,
        columns,
        new SQLRepresentation[] {hiveRep("SELECT id FROM t")},
        null,
        null,
        Collections.emptyMap());
    Assertions.assertTrue(viewCatalog.viewExists(ident));

    Assertions.assertTrue(viewCatalog.dropView(ident));
    Assertions.assertFalse(viewCatalog.viewExists(ident));

    // Dropping a non-existent view should return false
    Assertions.assertFalse(viewCatalog.dropView(ident));
  }

  @Test
  public void testAlterViewRename() {
    String viewName = GravitinoITUtils.genRandomName("hive_rename_view");
    String newName = GravitinoITUtils.genRandomName("hive_renamed_view");
    Column[] columns = {Column.of("id", Types.LongType.get(), null)};

    viewCatalog.createView(
        NameIdentifier.of(schemaName, viewName),
        null,
        columns,
        new SQLRepresentation[] {hiveRep("SELECT id FROM t")},
        null,
        null,
        Collections.emptyMap());

    View renamed =
        viewCatalog.alterView(NameIdentifier.of(schemaName, viewName), ViewChange.rename(newName));

    Assertions.assertEquals(newName, renamed.name());
    Assertions.assertFalse(viewCatalog.viewExists(NameIdentifier.of(schemaName, viewName)));
    Assertions.assertTrue(viewCatalog.viewExists(NameIdentifier.of(schemaName, newName)));
  }

  @Test
  public void testAlterViewSetAndRemoveProperty() {
    String viewName = GravitinoITUtils.genRandomName("hive_prop_view");
    Column[] columns = {Column.of("id", Types.LongType.get(), null)};

    viewCatalog.createView(
        NameIdentifier.of(schemaName, viewName),
        null,
        columns,
        new SQLRepresentation[] {hiveRep("SELECT id FROM t")},
        null,
        null,
        Collections.emptyMap());

    View withProp =
        viewCatalog.alterView(
            NameIdentifier.of(schemaName, viewName), ViewChange.setProperty("my_key", "my_value"));
    Assertions.assertEquals("my_value", withProp.properties().get("my_key"));

    View withoutProp =
        viewCatalog.alterView(
            NameIdentifier.of(schemaName, viewName), ViewChange.removeProperty("my_key"));
    Assertions.assertNull(withoutProp.properties().get("my_key"));
  }

  @Test
  public void testCreateViewAlreadyExists() {
    String viewName = GravitinoITUtils.genRandomName("hive_dup_view");
    NameIdentifier ident = NameIdentifier.of(schemaName, viewName);
    Column[] columns = {Column.of("id", Types.LongType.get(), null)};

    viewCatalog.createView(
        ident,
        null,
        columns,
        new SQLRepresentation[] {hiveRep("SELECT id FROM t")},
        null,
        null,
        Collections.emptyMap());

    Assertions.assertThrows(
        ViewAlreadyExistsException.class,
        () ->
            viewCatalog.createView(
                ident,
                null,
                columns,
                new SQLRepresentation[] {hiveRep("SELECT id FROM t")},
                null,
                null,
                Collections.emptyMap()));
  }

  @Test
  public void testAlterViewReplace() {
    String viewName = GravitinoITUtils.genRandomName("hive_replace_view");
    Column[] columns = {Column.of("id", Types.LongType.get(), null)};

    viewCatalog.createView(
        NameIdentifier.of(schemaName, viewName),
        "original comment",
        columns,
        new SQLRepresentation[] {hiveRep("SELECT id FROM t")},
        null,
        null,
        Collections.emptyMap());

    String updatedSql = "SELECT id FROM t WHERE id > 10";
    View updated =
        viewCatalog.alterView(
            NameIdentifier.of(schemaName, viewName),
            ViewChange.replaceView(
                columns,
                new SQLRepresentation[] {hiveRep(updatedSql)},
                null,
                null,
                "updated comment"));

    Assertions.assertEquals("updated comment", updated.comment());
    Assertions.assertEquals(1, updated.representations().length);
    Assertions.assertInstanceOf(SQLRepresentation.class, updated.representations()[0]);
    Assertions.assertEquals(updatedSql, ((SQLRepresentation) updated.representations()[0]).sql());
  }

  @Test
  public void testLoadNonExistentView() {
    Assertions.assertThrows(
        NoSuchViewException.class,
        () -> viewCatalog.loadView(NameIdentifier.of(schemaName, "non_existent_view_xyz")));
  }

  @Test
  public void testListViewsDoesNotIncludeTables() {
    String tableName = GravitinoITUtils.genRandomName("hive_not_a_view_table");
    String viewName = GravitinoITUtils.genRandomName("hive_actual_view");
    Column[] columns = {Column.of("id", Types.LongType.get(), null)};

    // Create a regular table
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            columns,
            "a regular table",
            Collections.emptyMap());

    // Create a view
    viewCatalog.createView(
        NameIdentifier.of(schemaName, viewName),
        null,
        columns,
        new SQLRepresentation[] {hiveRep("SELECT id FROM t")},
        null,
        null,
        Collections.emptyMap());

    NameIdentifier[] views = viewCatalog.listViews(Namespace.of(schemaName));
    for (NameIdentifier v : views) {
      Assertions.assertNotEquals(
          tableName, v.name(), "listViews should not include regular tables");
    }
    boolean found = false;
    for (NameIdentifier v : views) {
      if (v.name().equals(viewName)) found = true;
    }
    Assertions.assertTrue(found, "actual view not found in listViews");

    // Clean up table
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
  }

  @Test
  public void testLoadViewCreatedByHiveCli() {
    String tableName = GravitinoITUtils.genRandomName("hive_client_source_table");
    String viewName = GravitinoITUtils.genRandomName("hive_client_created_view");
    String qualifiedTableName = qualifyHiveObject(tableName);
    String qualifiedViewName = qualifyHiveObject(viewName);

    try {
      executeHiveSql(String.format("CREATE TABLE %s (id BIGINT, name STRING)", qualifiedTableName));
      executeHiveSql(
          String.format(
              "CREATE VIEW %s COMMENT '%s' AS SELECT id, name FROM %s",
              qualifiedViewName, VIEW_COMMENT, qualifiedTableName));

      View loaded = viewCatalog.loadView(NameIdentifier.of(schemaName, viewName));
      Assertions.assertEquals(viewName, loaded.name());
      Assertions.assertEquals(VIEW_COMMENT, loaded.comment());
      Assertions.assertEquals(1, loaded.representations().length);
      Assertions.assertInstanceOf(SQLRepresentation.class, loaded.representations()[0]);

      SQLRepresentation representation = (SQLRepresentation) loaded.representations()[0];
      Assertions.assertEquals(HIVE_DIALECT, representation.dialect());
      String normalizedSql = representation.sql().toLowerCase(Locale.ROOT);
      Assertions.assertTrue(normalizedSql.contains("select id, name"));
      Assertions.assertTrue(normalizedSql.contains(tableName.toLowerCase(Locale.ROOT)));
    } finally {
      executeHiveSql(String.format("DROP VIEW IF EXISTS %s", qualifiedViewName));
      executeHiveSql(String.format("DROP TABLE IF EXISTS %s", qualifiedTableName));
    }
  }

  @Test
  public void testHiveCliCanReadViewCreatedByGravitino() {
    String tableName = GravitinoITUtils.genRandomName("hive_client_read_source");
    String viewName = GravitinoITUtils.genRandomName("hive_client_read_view");
    String qualifiedTableName = qualifyHiveObject(tableName);
    String qualifiedViewName = qualifyHiveObject(viewName);
    Column[] columns = {
      Column.of("id", Types.LongType.get(), "id column"),
      Column.of("name", Types.StringType.get(), "name column")
    };

    try {
      executeHiveSql(String.format("CREATE TABLE %s (id BIGINT, name STRING)", qualifiedTableName));
      executeHiveSql(String.format("INSERT INTO TABLE %s VALUES (1, 'alice')", qualifiedTableName));
      executeHiveSql(String.format("INSERT INTO TABLE %s VALUES (2, 'bob')", qualifiedTableName));

      viewCatalog.createView(
          NameIdentifier.of(schemaName, viewName),
          VIEW_COMMENT,
          columns,
          new SQLRepresentation[] {
            hiveRep(String.format("SELECT id, name FROM %s", qualifiedTableName))
          },
          null,
          null,
          Collections.emptyMap());

      long rowCount = querySingleLong(String.format("SELECT COUNT(*) FROM %s", qualifiedViewName));
      Assertions.assertEquals(2L, rowCount);
    } finally {
      executeHiveSql(String.format("DROP VIEW IF EXISTS %s", qualifiedViewName));
      executeHiveSql(String.format("DROP TABLE IF EXISTS %s", qualifiedTableName));
    }
  }

  private SQLRepresentation hiveRep(String sql) {
    return SQLRepresentation.builder().withDialect(HIVE_DIALECT).withSql(sql).build();
  }

  private String qualifyHiveObject(String objectName) {
    return String.format("%s.%s", schemaName, objectName);
  }

  private void executeHiveSql(String sql) {
    ExecResult result =
        containerSuite.getHiveContainer().executeInContainer("hive", "-S", "-e", sql);
    Assertions.assertEquals(
        0,
        result.getExitCode(),
        String.format(
            "Failed to execute SQL with hive cli. SQL: %s, stdout: %s, stderr: %s",
            sql, result.getStdout(), result.getStderr()));
  }

  private long querySingleLong(String sql) {
    ExecResult result =
        containerSuite.getHiveContainer().executeInContainer("hive", "-S", "-e", sql);
    Assertions.assertEquals(
        0,
        result.getExitCode(),
        String.format(
            "Failed to query SQL with hive cli. SQL: %s, stdout: %s, stderr: %s",
            sql, result.getStdout(), result.getStderr()));

    String[] lines = result.getStdout().split("\\R");
    for (int index = lines.length - 1; index >= 0; index--) {
      String line = lines[index].trim();
      if (line.matches("-?\\d+")) {
        return Long.parseLong(line);
      }
    }
    throw new IllegalStateException(
        String.format(
            "Cannot parse numeric result from query output. SQL: %s, stdout: %s",
            sql, result.getStdout()));
  }
}
