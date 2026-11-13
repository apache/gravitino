/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.catalog.starrocks.operations;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.catalog.starrocks.StarRocksView;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.View;

/**
 * Table operations for StarRocks, extended with read-only access to native logical views. Views
 * cannot be created or altered through the catalog, so create, alter and drop fall back to the
 * {@code ViewCatalog} defaults and only listing and loading are implemented here.
 */
public class StarRocksViewOperations extends StarRocksTableOperations {

  /** Loads the name and comment of one view from {@code information_schema.tables}. */
  public static final String VIEW_SQL =
      "SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.tables "
          + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND TABLE_TYPE = 'VIEW'";

  /** Loads the {@code VIEW_DEFINITION} of one view from {@code information_schema.views}. */
  public static final String DEFINITION_SQL =
      "SELECT VIEW_DEFINITION FROM information_schema.views "
          + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

  /**
   * Logical views of one database. StarRocks also reports async materialized views as TABLE_TYPE
   * 'VIEW' in information_schema.tables, but only logical views have a row in
   * information_schema.views, so this is the set exposed through the view API.
   */
  public static final String LIST_VIEWS_SQL =
      "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.views WHERE TABLE_SCHEMA = ?";

  /** Lists the logical views of a database; async materialized views are not included. */
  public List<String> listViews(String databaseName) {
    List<String> names = new ArrayList<>();
    try (Connection connection = getConnection(databaseName);
        PreparedStatement statement = connection.prepareStatement(LIST_VIEWS_SQL)) {
      statement.setString(1, databaseName);
      try (ResultSet results = statement.executeQuery()) {
        while (results.next()) {
          // information_schema matching can be case-insensitive; keep exact matches only.
          if (databaseName.equals(results.getString("TABLE_SCHEMA"))) {
            names.add(results.getString("TABLE_NAME"));
          }
        }
      }
      return names;
    } catch (SQLException exception) {
      throw exceptionMapper.toGravitinoException(exception);
    }
  }

  /**
   * Table loads only see real tables. The base lookup passes no type filter, so a view name is
   * found and then fails later in {@code SHOW CREATE TABLE} with a SQL error instead of
   * NoSuchTableException. Semantic Model source validation (and any caller that tries a table
   * first) relies on NoSuchTableException to fall back to the view API. listTables already uses the
   * same TABLE-only filter.
   */
  @Override
  protected ResultSet getTable(Connection connection, String databaseName, String tableName)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getTables(
        connection.getCatalog(),
        connection.getSchema(),
        tableName,
        JdbcConnectorUtils.getTableTypes());
  }

  /**
   * Loads a native logical view: comment, output columns and SQL definition. Only real views are
   * read; ordinary tables must not acquire view privileges, and async materialized views
   * (TABLE_TYPE 'VIEW' but no information_schema.views row) are not views either, matching {@link
   * #listViews}.
   */
  public View loadView(String databaseName, String viewName) throws NoSuchViewException {
    try (Connection connection = getConnection(databaseName)) {
      String comment = findViewComment(connection, databaseName, viewName);
      String definition = readDefinition(connection, databaseName, viewName);
      if (definition == null) {
        throw new NoSuchViewException("View %s does not exist in %s", viewName, databaseName);
      }
      Column[] columns = readColumns(connection, databaseName, viewName);
      return new StarRocksView(viewName, comment, columns, definition, databaseName);
    } catch (SQLException exception) {
      throw exceptionMapper.toGravitinoException(exception);
    }
  }

  private String findViewComment(Connection connection, String databaseName, String viewName)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(VIEW_SQL)) {
      statement.setString(1, databaseName);
      statement.setString(2, viewName);
      try (ResultSet results = statement.executeQuery()) {
        while (results.next()) {
          // information_schema matching can be case-insensitive; require an exact name.
          if (viewName.equals(results.getString("TABLE_NAME"))) {
            return results.getString("TABLE_COMMENT");
          }
        }
      }
    }
    throw new NoSuchViewException("View %s does not exist in %s", viewName, databaseName);
  }

  private String readDefinition(Connection connection, String databaseName, String viewName)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(DEFINITION_SQL)) {
      statement.setString(1, databaseName);
      statement.setString(2, viewName);
      try (ResultSet results = statement.executeQuery()) {
        // No row: not a logical view (e.g. an async materialized view). A row with a null or blank
        // definition is still a view, just without a SQL representation.
        if (!results.next()) {
          return null;
        }
        String definition = results.getString("VIEW_DEFINITION");
        return definition == null ? "" : definition;
      }
    }
  }

  /** Same column mapping as table loads, so views and tables report identical types. */
  private Column[] readColumns(Connection connection, String databaseName, String viewName)
      throws SQLException {
    List<JdbcColumn> columns = new ArrayList<>();
    try (ResultSet results = getColumns(connection, databaseName, viewName)) {
      while (results.next()) {
        JdbcColumn.Builder builder = getColumnBuilder(results, databaseName, viewName);
        if (builder != null) {
          columns.add(builder.build());
        }
      }
    }
    return columns.toArray(new Column[0]);
  }
}
