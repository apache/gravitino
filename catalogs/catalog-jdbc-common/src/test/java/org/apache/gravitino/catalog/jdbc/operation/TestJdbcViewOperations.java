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
package org.apache.gravitino.catalog.jdbc.operation;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.gravitino.catalog.jdbc.JdbcView;
import org.apache.gravitino.catalog.jdbc.converter.SqliteExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteTypeConverter;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SQLRepresentation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/**
 * Tests the template methods in {@link JdbcViewOperations} against an in-memory SQLite database.
 */
@TestInstance(Lifecycle.PER_CLASS)
public class TestJdbcViewOperations {

  private SqliteViewOperations viewOps;
  private BasicDataSource dataSource;

  @BeforeAll
  void startDb() {
    dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:sqlite::memory:");
    dataSource.setMaxTotal(1);
    dataSource.setMinIdle(1);
    dataSource.setMaxIdle(1);

    viewOps = new SqliteViewOperations();
    viewOps.initialize(
        dataSource,
        new SqliteExceptionConverter(),
        new SqliteTypeConverter(),
        Collections.emptyMap());
  }

  @AfterAll
  void stopDb() throws Exception {
    if (dataSource != null) {
      dataSource.close();
    }
  }

  @BeforeEach
  void resetViews() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("DROP VIEW IF EXISTS \"test_view\"");
      stmt.execute("DROP VIEW IF EXISTS \"view_a\"");
      stmt.execute("DROP VIEW IF EXISTS \"view_b\"");
      stmt.execute("DROP VIEW IF EXISTS \"created_view\"");
      stmt.execute("DROP TABLE IF EXISTS \"base_table\"");
      stmt.execute("CREATE TABLE base_table (id INTEGER, name TEXT)");
    }
  }

  @Test
  public void testListViewsEmpty() {
    List<String> views = viewOps.listViews("main");
    Assertions.assertTrue(views.isEmpty());
  }

  @Test
  public void testListViewsFindsCreatedViews() throws SQLException {
    createRawView("view_a", "SELECT id FROM base_table");
    createRawView("view_b", "SELECT name FROM base_table");

    List<String> views = viewOps.listViews("main");
    Assertions.assertEquals(2, views.size());
    Assertions.assertTrue(views.contains("view_a"));
    Assertions.assertTrue(views.contains("view_b"));
  }

  @Test
  public void testLoadViewReturnsColumnsAndRepresentation() throws SQLException {
    createRawView("test_view", "SELECT id, name FROM base_table");

    JdbcView view = viewOps.load("main", "test_view");
    Assertions.assertEquals("test_view", view.name());

    Column[] columns = view.columns();
    Assertions.assertEquals(2, columns.length);
    Assertions.assertEquals("id", columns[0].name());
    Assertions.assertEquals("name", columns[1].name());

    Assertions.assertEquals(1, view.representations().length);
    SQLRepresentation rep = (SQLRepresentation) view.representations()[0];
    Assertions.assertEquals("sqlite", rep.dialect());
    Assertions.assertFalse(rep.sql().isEmpty());
  }

  @Test
  public void testLoadNonExistentViewThrows() {
    Assertions.assertThrows(NoSuchViewException.class, () -> viewOps.load("main", "no_such_view"));
  }

  @Test
  public void testDiscoverColumnsTypes() throws SQLException {
    createRawView("test_view", "SELECT id, name FROM base_table");

    JdbcView view = viewOps.load("main", "test_view");
    for (Column col : view.columns()) {
      Assertions.assertNotNull(col.dataType());
    }
  }

  @Test
  public void testCreateAndLoad() {
    viewOps.create("main", "created_view", null, "SELECT id FROM base_table");

    JdbcView view = viewOps.load("main", "created_view");
    Assertions.assertEquals("created_view", view.name());
    Assertions.assertEquals(1, view.columns().length);
    Assertions.assertEquals("id", view.columns()[0].name());
  }

  @Test
  public void testDropExistingViewReturnsTrue() throws SQLException {
    createRawView("test_view", "SELECT 1");
    Assertions.assertTrue(viewOps.drop("main", "test_view"));
    Assertions.assertThrows(NoSuchViewException.class, () -> viewOps.load("main", "test_view"));
  }

  @Test
  public void testDropNonExistentViewReturnsFalse() {
    Assertions.assertFalse(viewOps.drop("main", "no_such_view"));
  }

  private void createRawView(String name, String sql) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE VIEW \"" + name + "\" AS " + sql);
    }
  }
}
