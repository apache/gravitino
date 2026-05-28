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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcView;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SQLRepresentation;

/** Abstract base class for database-specific JDBC view operations. */
public abstract class JdbcViewOperations {

  protected DataSource dataSource;
  protected JdbcExceptionConverter exceptionMapper;
  protected JdbcTypeConverter typeConverter;

  /**
   * Initializes the view operations with the given data source and converters.
   *
   * @param dataSource The JDBC data source.
   * @param exceptionMapper The exception converter.
   * @param typeConverter The type converter.
   * @param conf The configuration map.
   */
  public void initialize(
      DataSource dataSource,
      JdbcExceptionConverter exceptionMapper,
      JdbcTypeConverter typeConverter,
      Map<String, String> conf) {
    this.dataSource = dataSource;
    this.exceptionMapper = exceptionMapper;
    this.typeConverter = typeConverter;
  }

  /**
   * Lists all view names in the given database/schema.
   *
   * @param databaseName The database or schema name.
   * @return A list of view names.
   */
  public List<String> listViews(String databaseName) {
    try (Connection connection = getConnection(databaseName)) {
      String sql = generateListViewsSql();
      try (PreparedStatement stmt = connection.prepareStatement(sql)) {
        bindListViewsParameters(stmt, databaseName);
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> views = new ArrayList<>();
          while (rs.next()) {
            views.add(rs.getString(1));
          }
          return views;
        }
      }
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
  }

  /**
   * Loads view metadata from the database.
   *
   * @param databaseName The database or schema name.
   * @param viewName The view name.
   * @return The loaded view.
   * @throws NoSuchViewException If the view does not exist.
   */
  public JdbcView load(String databaseName, String viewName) throws NoSuchViewException {
    try (Connection connection = getConnection(databaseName)) {
      String viewDefinition = loadViewDefinition(connection, databaseName, viewName);
      Preconditions.checkNotNull(
          viewDefinition, "View definition is null for view %s in %s", viewName, databaseName);
      Column[] columns = discoverColumns(connection, viewName);
      String comment = loadComment(connection, databaseName, viewName);

      SQLRepresentation rep =
          SQLRepresentation.builder().withDialect(dialectName()).withSql(viewDefinition).build();

      return JdbcView.builder()
          .withName(viewName)
          .withComment(comment)
          .withColumns(columns)
          .withRepresentations(new SQLRepresentation[] {rep})
          .withProperties(ImmutableMap.of())
          .build();
    } catch (NoSuchViewException e) {
      throw e;
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
  }

  /**
   * Returns the SQL dialect name for this database (e.g. "mysql", "postgresql").
   *
   * @return The dialect name.
   */
  public abstract String dialectName();

  /**
   * Returns a parameterized SQL template to list all views. Use {@code ?} placeholders for
   * parameters. Override {@link #bindListViewsParameters} to bind them.
   *
   * @return The SQL template string with {@code ?} placeholders.
   */
  protected abstract String generateListViewsSql();

  /**
   * Binds parameters for the list-views query. Default binds parameter 1 as databaseName.
   *
   * @param stmt The prepared statement.
   * @param databaseName The database or schema name.
   * @throws SQLException If parameter binding fails.
   */
  protected void bindListViewsParameters(PreparedStatement stmt, String databaseName)
      throws SQLException {
    stmt.setString(1, databaseName);
  }

  /**
   * Returns a parameterized SQL template to load a view definition. Use {@code ?} placeholders for
   * parameters. Override {@link #bindLoadViewParameters} to bind them.
   *
   * @return The SQL template string with {@code ?} placeholders.
   */
  protected abstract String generateLoadViewSql();

  /**
   * Binds parameters for the load-view query. Default binds parameter 1 as databaseName and
   * parameter 2 as viewName.
   *
   * @param stmt The prepared statement.
   * @param databaseName The database or schema name.
   * @param viewName The view name.
   * @throws SQLException If parameter binding fails.
   */
  protected void bindLoadViewParameters(
      PreparedStatement stmt, String databaseName, String viewName) throws SQLException {
    stmt.setString(1, databaseName);
    stmt.setString(2, viewName);
  }

  /**
   * Obtains a JDBC connection routed to the given database/schema.
   *
   * @param databaseName The target database or schema.
   * @return A connection routed to the given database.
   * @throws SQLException If a connection cannot be obtained.
   */
  protected Connection getConnection(String databaseName) throws SQLException {
    Connection connection = dataSource.getConnection();
    connection.setCatalog(databaseName);
    return connection;
  }

  /**
   * Quotes an identifier according to the database's quoting convention.
   *
   * @param identifier The identifier to quote.
   * @return The quoted identifier.
   */
  protected abstract String quoteIdentifier(String identifier);

  /**
   * Loads the comment for a view from the database. The default implementation returns {@code null}
   * for databases that do not support view comments (e.g., MySQL). Subclasses should override this
   * to read comments from the backend (e.g., PostgreSQL's {@code pg_description}).
   *
   * @param connection The JDBC connection.
   * @param databaseName The database or schema name.
   * @param viewName The view name.
   * @return The view comment, or {@code null} if not available.
   * @throws SQLException If the comment cannot be loaded.
   */
  protected String loadComment(Connection connection, String databaseName, String viewName)
      throws SQLException {
    return null;
  }

  private String loadViewDefinition(Connection connection, String databaseName, String viewName)
      throws SQLException, NoSuchViewException {
    String sql = generateLoadViewSql();
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      bindLoadViewParameters(stmt, databaseName, viewName);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getString(1);
        }
        throw new NoSuchViewException("View %s does not exist in %s", viewName, databaseName);
      }
    }
  }

  private Column[] discoverColumns(Connection connection, String viewName) throws SQLException {
    String quotedView = quoteIdentifier(viewName);
    String sql = "SELECT * FROM " + quotedView + " WHERE 1=0";
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      ResultSetMetaData metaData = rs.getMetaData();
      int columnCount = metaData.getColumnCount();
      Column[] columns = new Column[columnCount];
      for (int i = 1; i <= columnCount; i++) {
        String colName = metaData.getColumnName(i);
        String typeName = metaData.getColumnTypeName(i);
        int precision = metaData.getPrecision(i);
        boolean nullable = metaData.isNullable(i) != ResultSetMetaData.columnNoNulls;

        int scale = metaData.getScale(i);

        JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean(typeName);
        typeBean.setColumnSize(precision);
        typeBean.setScale(scale);

        columns[i - 1] =
            JdbcColumn.builder()
                .withName(colName)
                .withType(typeConverter.toGravitino(typeBean))
                .withNullable(nullable)
                .build();
      }
      return columns;
    }
  }
}
