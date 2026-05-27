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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.jdbc.operation.JdbcViewOperations;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
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
        Arrays.stream(result).map(NameIdentifier::name).sorted().collect(Collectors.toList());
    Assertions.assertEquals(Arrays.asList("v1", "v2"), names);
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
  public void testJdbcViewPropertiesNeverNull() {
    JdbcView view =
        JdbcView.builder()
            .withName("v")
            .withRepresentations(
                new SQLRepresentation[] {
                  SQLRepresentation.builder().withDialect("test").withSql("SELECT 1").build()
                })
            .build();
    Assertions.assertNotNull(view.properties(), "properties() must not return null");
  }

  /** In-memory stub of {@link JdbcViewOperations} for testing without a real database. */
  private static class StubViewOperations extends JdbcViewOperations {

    final Map<String, Map<String, String>> views = new HashMap<>();

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
    public String dialectName() {
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
    protected String quoteIdentifier(String identifier) {
      return "\"" + identifier + "\"";
    }

    @Override
    protected Connection getConnection(String databaseName) {
      throw new UnsupportedOperationException();
    }
  }
}
