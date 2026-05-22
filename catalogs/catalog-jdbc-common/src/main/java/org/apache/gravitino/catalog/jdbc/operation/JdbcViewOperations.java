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

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcView;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base operations for reading views from a JDBC data store. */
public abstract class JdbcViewOperations implements ViewOperation {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcViewOperations.class);

  private static final Pattern VIEW_SELECT_BODY_PATTERN =
      Pattern.compile("(?is)\\s+AS\\s+(.*)\\s*$");

  protected DataSource dataSource;
  protected JdbcExceptionConverter exceptionMapper;
  protected JdbcTypeConverter typeConverter;

  @Override
  public void initialize(
      DataSource dataSource,
      JdbcExceptionConverter exceptionMapper,
      JdbcTypeConverter jdbcTypeConverter,
      Map<String, String> conf) {
    this.dataSource = dataSource;
    this.exceptionMapper = exceptionMapper;
    this.typeConverter = jdbcTypeConverter;
  }

  @Override
  public List<String> listViews(String databaseName) throws NoSuchSchemaException {
    final List<String> names = Lists.newArrayList();
    try (Connection connection = getConnection(databaseName);
        ResultSet views = getViews(connection)) {
      while (views.next()) {
        if (matchesDatabase(views, databaseName)) {
          names.add(views.getString("TABLE_NAME"));
        }
      }
      LOG.info("Finished listing views size {} for database name {}", names.size(), databaseName);
      return names;
    } catch (final SQLException se) {
      throw exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public JdbcView load(String databaseName, String viewName) throws NoSuchViewException {
    try (Connection connection = getConnection(databaseName)) {
      String comment = null;
      try (ResultSet views = getView(connection, databaseName, viewName)) {
        if (!views.next() || !Objects.equals(views.getString("TABLE_NAME"), viewName)) {
          throw new NoSuchViewException("View %s does not exist in %s.", viewName, databaseName);
        }
        comment = views.getString("REMARKS");
        if (StringUtils.isEmpty(comment)) {
          comment = null;
        }
      }

      String viewSql = loadViewDefinition(connection, databaseName, viewName);
      Column[] columns = loadViewColumns(connection, databaseName, viewName);
      SQLRepresentation representation =
          SQLRepresentation.builder().withDialect(getSqlDialect()).withSql(viewSql).build();

      return JdbcView.builder()
          .withName(viewName)
          .withComment(comment)
          .withDefaultCatalog(null)
          .withDefaultSchema(databaseName)
          .withColumns(columns)
          .withRepresentations(new Representation[] {representation})
          .withAuditInfo(AuditInfo.EMPTY)
          .build();
    } catch (NoSuchViewException e) {
      throw e;
    } catch (final SQLException se) {
      throw exceptionMapper.toGravitinoException(se);
    }
  }

  /**
   * Returns the SQL dialect identifier used in {@link SQLRepresentation} for this JDBC catalog.
   *
   * @return The dialect identifier, for example {@code "mysql"}.
   */
  protected abstract String getSqlDialect();

  /**
   * Loads the SQL text of a view from the underlying database.
   *
   * @param connection The JDBC connection scoped to the database or schema.
   * @param databaseName The database or schema name.
   * @param viewName The view name.
   * @return The SQL text of the view definition.
   * @throws SQLException If a database access error occurs.
   * @throws NoSuchViewException If the view definition cannot be found.
   */
  protected abstract String loadViewDefinition(
      Connection connection, String databaseName, String viewName)
      throws SQLException, NoSuchViewException;

  /**
   * Quotes an identifier for use in SQL statements.
   *
   * @param identifier The identifier to quote.
   * @return The quoted identifier.
   */
  protected abstract String quoteIdentifier(String identifier);

  /**
   * Returns a JDBC connection scoped to the given database or schema.
   *
   * @param databaseName The database or schema name.
   * @return A JDBC connection.
   * @throws SQLException If a database access error occurs.
   */
  protected Connection getConnection(String databaseName) throws SQLException {
    Connection connection = dataSource.getConnection();
    connection.setCatalog(databaseName);
    return connection;
  }

  /**
   * Returns whether the metadata row belongs to the requested database or schema. The default
   * implementation matches {@code TABLE_CAT}; subclasses may override for multi-schema catalogs.
   *
   * @param views The view metadata result set positioned on a row.
   * @param databaseName The database or schema name.
   * @return {@code true} if the row matches the requested namespace.
   * @throws SQLException If a database access error occurs.
   */
  protected boolean matchesDatabase(ResultSet views, String databaseName) throws SQLException {
    return Objects.equals(views.getString("TABLE_CAT"), databaseName);
  }

  /**
   * Returns a {@link ResultSet} of all views in the current catalog/schema via {@link
   * DatabaseMetaData#getTables}.
   *
   * @param connection The JDBC connection.
   * @return A result set of view metadata rows.
   * @throws SQLException If a database access error occurs.
   */
  protected ResultSet getViews(Connection connection) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getTables(
        connection.getCatalog(), connection.getSchema(), null, JdbcConnectorUtils.getViewTypes());
  }

  /**
   * Returns a {@link ResultSet} for a single named view via {@link DatabaseMetaData#getTables}.
   *
   * @param connection The JDBC connection.
   * @param databaseName The database or schema name (used only for error context).
   * @param viewName The view name to look up.
   * @return A result set of view metadata rows matching the given name.
   * @throws SQLException If a database access error occurs.
   */
  protected ResultSet getView(Connection connection, String databaseName, String viewName)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getTables(
        connection.getCatalog(),
        connection.getSchema(),
        viewName,
        JdbcConnectorUtils.getViewTypes());
  }

  /**
   * Extracts the SELECT body from a {@code CREATE VIEW} statement.
   *
   * @param createViewStatement The full {@code CREATE VIEW} statement.
   * @return The SQL text after {@code AS}, or the original statement if parsing fails.
   */
  protected String extractSelectBodyFromCreateView(String createViewStatement) {
    if (StringUtils.isBlank(createViewStatement)) {
      return createViewStatement;
    }
    Matcher matcher = VIEW_SELECT_BODY_PATTERN.matcher(createViewStatement.trim());
    if (matcher.find()) {
      return matcher.group(1).trim();
    }
    return createViewStatement.trim();
  }

  /**
   * Loads view output columns via {@code SELECT * FROM view WHERE 1=0} and {@link
   * ResultSetMetaData}.
   *
   * @param connection The JDBC connection.
   * @param databaseName The database or schema name.
   * @param viewName The view name.
   * @return The view output columns.
   * @throws SQLException If a database access error occurs.
   */
  protected Column[] loadViewColumns(Connection connection, String databaseName, String viewName)
      throws SQLException {
    String sql = String.format("SELECT * FROM %s WHERE 1=0", quoteIdentifier(viewName));
    try (PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery()) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      List<Column> columns = new ArrayList<>();
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        columns.add(buildColumnFromMetaData(metaData, i));
      }
      return columns.toArray(new Column[0]);
    } catch (SQLException e) {
      LOG.warn(
          "Failed to introspect columns for view {} in {} via empty SELECT: {}",
          viewName,
          databaseName,
          e.getMessage());
      return new Column[0];
    }
  }

  /**
   * Builds a Gravitino column from {@link ResultSetMetaData}.
   *
   * @param metaData The result set metadata.
   * @param columnIndex The 1-based column index.
   * @return A Gravitino column.
   * @throws SQLException If a database access error occurs.
   */
  protected Column buildColumnFromMetaData(ResultSetMetaData metaData, int columnIndex)
      throws SQLException {
    String typeName = metaData.getColumnTypeName(columnIndex);
    int columnSize = metaData.getPrecision(columnIndex);
    int scale = metaData.getScale(columnIndex);
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean(typeName);
    typeBean.setColumnSize(columnSize);
    typeBean.setScale(scale);

    return JdbcColumn.builder()
        .withName(metaData.getColumnLabel(columnIndex))
        .withType(typeConverter.toGravitino(typeBean))
        .withNullable(true)
        .withDefaultValue(DEFAULT_VALUE_NOT_SET)
        .build();
  }

  /**
   * Loads a view definition from {@code information_schema.views}, used as a fallback for catalogs
   * without a dedicated loader.
   *
   * @param connection The JDBC connection.
   * @param databaseName The database or schema name.
   * @param viewName The view name.
   * @return The view definition SQL text.
   * @throws SQLException If a database access error occurs.
   * @throws NoSuchViewException If the view definition cannot be found.
   */
  protected String loadViewDefinitionFromInformationSchema(
      Connection connection, String databaseName, String viewName)
      throws SQLException, NoSuchViewException {
    String sql =
        "SELECT VIEW_DEFINITION FROM information_schema.views "
            + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, databaseName);
      statement.setString(2, viewName);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          throw new NoSuchViewException("View %s does not exist in %s.", viewName, databaseName);
        }
        String definition = resultSet.getString("VIEW_DEFINITION");
        if (StringUtils.isBlank(definition)) {
          throw new NoSuchViewException(
              "View definition for %s in %s is empty.", viewName, databaseName);
        }
        return definition.trim();
      }
    }
  }
}
