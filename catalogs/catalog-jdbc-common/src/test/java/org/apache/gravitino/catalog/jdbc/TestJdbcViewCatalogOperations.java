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
package org.apache.gravitino.catalog.jdbc;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.jdbc.operation.JdbcViewOperations;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link JdbcViewCatalogOperations}. */
public class TestJdbcViewCatalogOperations {

  private StubViewOperations stubViewOps;
  private JdbcViewCatalogOperations catalogOps;

  @BeforeEach
  void setUp() {
    stubViewOps = new StubViewOperations();
    catalogOps = new JdbcViewCatalogOperations(stubViewOps, schema -> "existing_db".equals(schema));
  }

  @Test
  public void testListViewsInExistingSchema() {
    stubViewOps.addView("existing_db", "v1", "SELECT 1");
    stubViewOps.addView("existing_db", "v2", "SELECT 2");

    NameIdentifier[] result = catalogOps.listViews(Namespace.of("existing_db"));

    Assertions.assertEquals(2, result.length);
    List<String> names =
        java.util.Arrays.stream(result)
            .map(NameIdentifier::name)
            .sorted()
            .collect(java.util.stream.Collectors.toList());
    Assertions.assertEquals(java.util.Arrays.asList("v1", "v2"), names);
  }

  @Test
  public void testListViewsInNonExistentSchema() {
    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> catalogOps.listViews(Namespace.of("no_such_db")));
  }

  @Test
  public void testLoadView() {
    stubViewOps.addView("existing_db", "my_view", "SELECT id FROM users");

    View loaded = catalogOps.loadView(NameIdentifier.of(Namespace.of("existing_db"), "my_view"));

    Assertions.assertEquals("my_view", loaded.name());
    Assertions.assertEquals(1, loaded.representations().length);
  }

  @Test
  public void testLoadNonExistentView() {
    Assertions.assertThrows(
        NoSuchViewException.class,
        () -> catalogOps.loadView(NameIdentifier.of(Namespace.of("existing_db"), "no_view")));
  }

  @Test
  public void testCreateViewInNonExistentSchema() {
    NameIdentifier ident = NameIdentifier.of(Namespace.of("no_such_db"), "v1");
    SQLRepresentation rep =
        SQLRepresentation.builder().withDialect("mysql").withSql("SELECT 1").build();

    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () ->
            catalogOps.createView(
                ident, "comment", new Column[0], new Representation[] {rep}, null, null, null));
    Assertions.assertTrue(stubViewOps.created.isEmpty());
  }

  @Test
  public void testCreateViewWithNoRepresentations() {
    NameIdentifier ident = NameIdentifier.of(Namespace.of("existing_db"), "v1");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            catalogOps.createView(
                ident, "comment", new Column[0], new Representation[0], null, null, null));
  }

  @Test
  public void testCreateViewWithNullRepresentations() {
    NameIdentifier ident = NameIdentifier.of(Namespace.of("existing_db"), "v1");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> catalogOps.createView(ident, "comment", new Column[0], null, null, null, null));
  }

  @Test
  public void testAlterViewSetPropertyThrows() {
    NameIdentifier ident = NameIdentifier.of(Namespace.of("existing_db"), "v1");

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> catalogOps.alterView(ident, ViewChange.setProperty("key", "value")));
  }

  @Test
  public void testAlterViewRemovePropertyThrows() {
    NameIdentifier ident = NameIdentifier.of(Namespace.of("existing_db"), "v1");

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> catalogOps.alterView(ident, ViewChange.removeProperty("key")));
  }

  @Test
  public void testAlterViewRename() {
    stubViewOps.addView("existing_db", "old_view", "SELECT 1");

    View result =
        catalogOps.alterView(
            NameIdentifier.of(Namespace.of("existing_db"), "old_view"),
            ViewChange.rename("new_view"));

    Assertions.assertEquals("new_view", result.name());
    Assertions.assertFalse(stubViewOps.views.get("existing_db").containsKey("old_view"));
    Assertions.assertTrue(stubViewOps.views.get("existing_db").containsKey("new_view"));
  }

  @Test
  public void testAlterViewReplace() {
    stubViewOps.addView("existing_db", "v1", "SELECT 1");

    Column[] newColumns = new Column[] {Column.of("id", Types.IntegerType.get(), "id column")};
    SQLRepresentation newRep =
        SQLRepresentation.builder().withDialect("mysql").withSql("SELECT id FROM t").build();

    View result =
        catalogOps.alterView(
            NameIdentifier.of(Namespace.of("existing_db"), "v1"),
            ViewChange.replaceView(
                newColumns, new Representation[] {newRep}, null, null, "new comment"));

    Assertions.assertEquals("v1", result.name());
    Assertions.assertEquals("SELECT id FROM t", stubViewOps.views.get("existing_db").get("v1"));
  }

  @Test
  public void testDropView() {
    stubViewOps.addView("existing_db", "v1", "SELECT 1");

    Assertions.assertTrue(
        catalogOps.dropView(NameIdentifier.of(Namespace.of("existing_db"), "v1")));
    Assertions.assertFalse(stubViewOps.views.get("existing_db").containsKey("v1"));
  }

  @Test
  public void testDropNonExistentView() {
    Assertions.assertFalse(
        catalogOps.dropView(NameIdentifier.of(Namespace.of("existing_db"), "no_view")));
  }

  @Test
  public void testViewExists() {
    stubViewOps.addView("existing_db", "v1", "SELECT 1");

    Assertions.assertTrue(
        catalogOps.viewExists(NameIdentifier.of(Namespace.of("existing_db"), "v1")));
  }

  @Test
  public void testViewDoesNotExist() {
    Assertions.assertFalse(
        catalogOps.viewExists(NameIdentifier.of(Namespace.of("existing_db"), "no_view")));
  }

  @Test
  public void testCreateViewDelegatesCorrectSql() {
    NameIdentifier ident = NameIdentifier.of(Namespace.of("existing_db"), "v1");
    SQLRepresentation rep =
        SQLRepresentation.builder()
            .withDialect("mysql")
            .withSql("SELECT id, name FROM users")
            .build();

    catalogOps.createView(
        ident, "test comment", new Column[0], new Representation[] {rep}, null, null, null);

    Assertions.assertEquals(1, stubViewOps.created.size());
    Assertions.assertEquals("SELECT id, name FROM users", stubViewOps.created.get(0).sql);
    Assertions.assertEquals("test comment", stubViewOps.created.get(0).comment);
    Assertions.assertEquals("v1", stubViewOps.created.get(0).viewName);
  }

  @Test
  public void testAlterViewRejectsUnsupportedChangesBeforeApplyingOthers() {
    stubViewOps.addView("existing_db", "v1", "SELECT 1");

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            catalogOps.alterView(
                NameIdentifier.of(Namespace.of("existing_db"), "v1"),
                ViewChange.rename("v2"),
                ViewChange.setProperty("key", "value")));

    Assertions.assertTrue(
        stubViewOps.views.get("existing_db").containsKey("v1"),
        "View should NOT have been renamed when a later change is unsupported");
    Assertions.assertFalse(
        stubViewOps.views.get("existing_db").containsKey("v2"),
        "Renamed view should not exist when the batch was rejected");
  }

  @Test
  public void testCreateViewReturnsDefaultCatalogAndSchema() {
    NameIdentifier ident = NameIdentifier.of(Namespace.of("existing_db"), "v1");
    SQLRepresentation rep =
        SQLRepresentation.builder().withDialect("mysql").withSql("SELECT 1").build();

    View created =
        catalogOps.createView(
            ident,
            null,
            new Column[0],
            new Representation[] {rep},
            "my_catalog",
            "my_schema",
            null);

    Assertions.assertEquals("my_catalog", created.defaultCatalog());
    Assertions.assertEquals("my_schema", created.defaultSchema());
  }

  @Test
  public void testJdbcViewPropertiesNeverNull() {
    JdbcView view = JdbcView.builder().withName("v").build();
    Assertions.assertNotNull(view.properties(), "properties() must not return null");
  }

  @Test
  public void testCreateViewReturnsComment() {
    NameIdentifier ident = NameIdentifier.of(Namespace.of("existing_db"), "v1");
    SQLRepresentation rep =
        SQLRepresentation.builder().withDialect("mysql").withSql("SELECT 1").build();

    View created =
        catalogOps.createView(
            ident, "my comment", new Column[0], new Representation[] {rep}, null, null, null);

    Assertions.assertEquals("my comment", created.comment());
  }

  @Test
  public void testAlterViewRenameAndReplace() {
    stubViewOps.addView("existing_db", "v1", "SELECT 1");

    Column[] cols = new Column[] {Column.of("id", Types.IntegerType.get(), "id")};
    SQLRepresentation newRep =
        SQLRepresentation.builder().withDialect("mysql").withSql("SELECT id FROM t").build();

    View result =
        catalogOps.alterView(
            NameIdentifier.of(Namespace.of("existing_db"), "v1"),
            ViewChange.rename("v2"),
            ViewChange.replaceView(cols, new Representation[] {newRep}, null, null, null));

    Assertions.assertEquals("v2", result.name());
    Assertions.assertFalse(stubViewOps.views.get("existing_db").containsKey("v1"));
    Assertions.assertEquals("SELECT id FROM t", stubViewOps.views.get("existing_db").get("v2"));
  }

  /** In-memory stub of {@link JdbcViewOperations} for testing without a real database. */
  private static class StubViewOperations extends JdbcViewOperations {

    final Map<String, Map<String, String>> views = new HashMap<>();
    final List<CreateRecord> created = new ArrayList<>();

    void addView(String db, String viewName, String sql) {
      views.computeIfAbsent(db, k -> new HashMap<>()).put(viewName, sql);
    }

    @Override
    public List<String> listViews(String databaseName) {
      Map<String, String> dbViews = views.getOrDefault(databaseName, new HashMap<>());
      return new ArrayList<>(dbViews.keySet());
    }

    @Override
    public JdbcView load(String databaseName, String viewName) throws NoSuchViewException {
      Map<String, String> dbViews = views.get(databaseName);
      if (dbViews == null || !dbViews.containsKey(viewName)) {
        throw new NoSuchViewException("View %s not found in %s", viewName, databaseName);
      }
      SQLRepresentation rep =
          SQLRepresentation.builder()
              .withDialect(dialectName())
              .withSql(dbViews.get(viewName))
              .build();
      return JdbcView.builder()
          .withName(viewName)
          .withColumns(new Column[0])
          .withRepresentations(new SQLRepresentation[] {rep})
          .withProperties(ImmutableMap.of())
          .build();
    }

    @Override
    public void create(String databaseName, String viewName, String comment, String sql)
        throws ViewAlreadyExistsException {
      created.add(new CreateRecord(databaseName, viewName, comment, sql));
      addView(databaseName, viewName, sql);
    }

    @Override
    public void replaceDefinition(String databaseName, String viewName, String comment, String sql)
        throws NoSuchViewException {
      Map<String, String> dbViews = views.get(databaseName);
      if (dbViews == null || !dbViews.containsKey(viewName)) {
        throw new NoSuchViewException("View %s not found", viewName);
      }
      dbViews.put(viewName, sql);
    }

    @Override
    public void rename(String databaseName, String oldName, String newName)
        throws NoSuchViewException {
      Map<String, String> dbViews = views.get(databaseName);
      if (dbViews == null || !dbViews.containsKey(oldName)) {
        throw new NoSuchViewException("View %s not found", oldName);
      }
      String sql = dbViews.remove(oldName);
      dbViews.put(newName, sql);
    }

    @Override
    public boolean drop(String databaseName, String viewName) {
      Map<String, String> dbViews = views.get(databaseName);
      if (dbViews == null) {
        return false;
      }
      return dbViews.remove(viewName) != null;
    }

    @Override
    protected String dialectName() {
      return "stub";
    }

    @Override
    protected String generateListViewsSql() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected String generateLoadViewSql() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected String generateCreateViewSql(String viewName, String sql) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected String generateReplaceViewSql(String viewName, String sql) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected String generateRenameViewSql(String oldName, String newName) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected String generateDropViewSql(String viewName) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected String quoteIdentifier(String identifier) {
      return "\"" + identifier + "\"";
    }

    @Override
    protected Connection getConnection(String databaseName) {
      throw new UnsupportedOperationException();
    }

    static class CreateRecord {
      final String databaseName;
      final String viewName;
      final String comment;
      final String sql;

      CreateRecord(String databaseName, String viewName, String comment, String sql) {
        this.databaseName = databaseName;
        this.viewName = viewName;
        this.comment = comment;
        this.sql = sql;
      }
    }
  }
}
