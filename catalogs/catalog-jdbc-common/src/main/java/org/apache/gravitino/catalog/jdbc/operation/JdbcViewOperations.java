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
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SQLRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Abstract base class for database-specific JDBC view operations. */
public abstract class JdbcViewOperations {

  protected static final Logger LOG = LoggerFactory.getLogger(JdbcViewOperations.class);

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
      Column[] columns = discoverColumns(connection, viewName);

      SQLRepresentation rep =
          SQLRepresentation.builder()
              .withDialect(dialectName())
              .withSql(viewDefinition != null ? viewDefinition : "")
              .build();

      return JdbcView.builder()
          .withName(viewName)
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
   * Creates a view in the database.
   *
   * @param databaseName The database or schema name.
   * @param viewName The view name.
   * @param comment The optional view comment.
   * @param sql The view definition SQL.
   * @throws ViewAlreadyExistsException If the view already exists.
   */
  public void create(String databaseName, String viewName, String comment, String sql)
      throws ViewAlreadyExistsException {
    try (Connection connection = getConnection(databaseName);
        Statement stmt = connection.createStatement()) {
      stmt.execute(generateCreateViewSql(viewName, sql));
      setComment(connection, viewName, comment);
      LOG.info("Created JDBC view {}.{}", databaseName, viewName);
    } catch (SQLException e) {
      GravitinoRuntimeException ge = exceptionMapper.toGravitinoException(e);
      if (ge instanceof TableAlreadyExistsException) {
        throw new ViewAlreadyExistsException(
            e, "View %s already exists in %s", viewName, databaseName);
      }
      throw ge;
    }
  }

  /**
   * Replaces the definition of an existing view.
   *
   * @param databaseName The database or schema name.
   * @param viewName The view name.
   * @param comment The optional view comment.
   * @param sql The new view definition SQL.
   * @throws NoSuchViewException If the view does not exist.
   */
  public void replaceDefinition(String databaseName, String viewName, String comment, String sql)
      throws NoSuchViewException {
    try (Connection connection = getConnection(databaseName);
        Statement stmt = connection.createStatement()) {
      stmt.execute(generateReplaceViewSql(viewName, sql));
      setComment(connection, viewName, comment);
      LOG.info("Replaced definition of JDBC view {}.{}", databaseName, viewName);
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
  }

  /**
   * Renames a view.
   *
   * @param databaseName The database or schema name.
   * @param oldName The current view name.
   * @param newName The new view name.
   * @throws NoSuchViewException If the view does not exist.
   */
  public void rename(String databaseName, String oldName, String newName)
      throws NoSuchViewException {
    try (Connection connection = getConnection(databaseName);
        Statement stmt = connection.createStatement()) {
      stmt.execute(generateRenameViewSql(oldName, newName));
      LOG.info("Renamed JDBC view {}.{} to {}", databaseName, oldName, newName);
    } catch (SQLException e) {
      GravitinoRuntimeException ge = exceptionMapper.toGravitinoException(e);
      if (ge instanceof NoSuchTableException) {
        throw new NoSuchViewException("View %s does not exist in %s", oldName, databaseName);
      }
      throw ge;
    }
  }

  /**
   * Drops a view from the database.
   *
   * @param databaseName The database or schema name.
   * @param viewName The view name.
   * @return {@code true} if the view was dropped, {@code false} if it did not exist.
   */
  public boolean drop(String databaseName, String viewName) {
    try (Connection connection = getConnection(databaseName)) {
      if (!viewExists(connection, databaseName, viewName)) {
        return false;
      }
      try (Statement stmt = connection.createStatement()) {
        stmt.execute(generateDropViewSql(viewName));
        LOG.info("Dropped JDBC view {}.{}", databaseName, viewName);
        return true;
      }
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
  }

  /**
   * Returns the SQL dialect name for this database (e.g. "mysql", "postgresql").
   *
   * @return The dialect name.
   */
  protected abstract String dialectName();

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
   * Generates a CREATE VIEW SQL statement.
   *
   * @param viewName The view name (will be quoted by the implementation).
   * @param sql The view definition SQL.
   * @return The CREATE VIEW DDL string.
   */
  protected abstract String generateCreateViewSql(String viewName, String sql);

  /**
   * Generates a CREATE OR REPLACE VIEW SQL statement.
   *
   * @param viewName The view name.
   * @param sql The new view definition SQL.
   * @return The DDL string.
   */
  protected abstract String generateReplaceViewSql(String viewName, String sql);

  /**
   * Generates a rename view SQL statement.
   *
   * @param oldName The current view name.
   * @param newName The new view name.
   * @return The DDL string.
   */
  protected abstract String generateRenameViewSql(String oldName, String newName);

  /**
   * Generates a DROP VIEW SQL statement.
   *
   * @param viewName The view name.
   * @return The DDL string.
   */
  protected abstract String generateDropViewSql(String viewName);

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
   * Sets a comment on the view if the database supports it. Default implementation is a no-op.
   *
   * @param connection The JDBC connection.
   * @param viewName The view name.
   * @param comment The comment text, may be {@code null}.
   * @throws SQLException If the comment cannot be set.
   */
  protected void setComment(Connection connection, String viewName, String comment)
      throws SQLException {
    // Default no-op; subclasses can override for databases that support view comments
  }

  private boolean viewExists(Connection connection, String databaseName, String viewName)
      throws SQLException {
    String sql = generateLoadViewSql();
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      bindLoadViewParameters(stmt, databaseName, viewName);
      try (ResultSet rs = stmt.executeQuery()) {
        return rs.next();
      }
    }
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
    try (PreparedStatement stmt = connection.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery()) {
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

  /**
   * Quotes an identifier according to the database's quoting convention.
   *
   * @param identifier The identifier to quote.
   * @return The quoted identifier.
   */
  protected abstract String quoteIdentifier(String identifier);
}
