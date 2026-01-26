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
package org.apache.gravitino.catalog.hologres.operation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.DatabaseOperation;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.operation.RequireDatabaseOperation;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

/**
 * Table operations for Hologres.
 *
 * <p>Hologres is compatible with PostgreSQL protocol, so this implementation extends the standard
 * PostgreSQL table operations with Hologres-specific behaviors.
 *
 * <p>Key differences from standard PostgreSQL:
 *
 * <ul>
 *   <li>Hologres is optimized for OLAP workloads, not OLTP
 *   <li>Hologres has different system table structures
 *   <li>Hologres supports specific table types like ORC, Parquet (foreign tables)
 * </ul>
 */
public class HologresTableOperations extends JdbcTableOperations
    implements RequireDatabaseOperation {

  public static final String HOLO_QUOTE = "\"";
  public static final String NEW_LINE = "\n";
  public static final String ALTER_TABLE = "ALTER TABLE ";
  public static final String ALTER_COLUMN = "ALTER COLUMN ";
  public static final String IS = " IS '";
  public static final String COLUMN_COMMENT = "COMMENT ON COLUMN ";
  public static final String TABLE_COMMENT = "COMMENT ON TABLE ";

  private String database;
  private HologresSchemaOperations schemaOperations;

  @Override
  public void initialize(
      DataSource dataSource,
      JdbcExceptionConverter exceptionMapper,
      JdbcTypeConverter jdbcTypeConverter,
      JdbcColumnDefaultValueConverter jdbcColumnDefaultValueConverter,
      Map<String, String> conf) {
    super.initialize(
        dataSource, exceptionMapper, jdbcTypeConverter, jdbcColumnDefaultValueConverter, conf);
    database = new JdbcConfig(conf).getJdbcDatabase();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(database),
        "The `jdbc-database` configuration item is mandatory in Hologres.");
  }

  @Override
  public void setDatabaseOperation(DatabaseOperation databaseOperation) {
    this.schemaOperations = (HologresSchemaOperations) databaseOperation;
  }

  @Override
  protected Connection getConnection(String schema) throws SQLException {
    Connection connection = dataSource.getConnection();
    connection.setCatalog(database);
    connection.setSchema(schema);
    return connection;
  }

  @Override
  public List<String> listTables(String schemaName) throws NoSuchSchemaException {
    try (Connection connection = getConnection(schemaName)) {
      if (!schemaOperations.schemaExists(connection, schemaName)) {
        throw new NoSuchSchemaException("No such schema: %s", schemaName);
      }
      final List<String> names = Lists.newArrayList();
      try (java.sql.ResultSet tables = getTables(connection)) {
        while (tables.next()) {
          if (Objects.equals(tables.getString("TABLE_SCHEM"), schemaName)) {
            names.add(tables.getString("TABLE_NAME"));
          }
        }
      }
      LOG.info("Finished listing tables size {} for schema name {}", names.size(), schemaName);
      return names;
    } catch (java.sql.SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  /**
   * Override to support TABLE, VIEW, and FOREIGN TABLE types in Hologres. Hologres supports views
   * and foreign tables which should be listed alongside regular tables.
   */
  @Override
  protected java.sql.ResultSet getTables(Connection connection) throws SQLException {
    final java.sql.DatabaseMetaData metaData = connection.getMetaData();
    String catalogName = connection.getCatalog();
    String schemaName = connection.getSchema();
    // Include TABLE, VIEW, and FOREIGN TABLE types
    String[] tableTypes = {"TABLE", "VIEW", "FOREIGN TABLE"};
    return metaData.getTables(catalogName, schemaName, null, tableTypes);
  }

  /**
   * Get Hologres-specific table properties from hologres.hg_table_properties system table.
   *
   * <p>This includes properties like distribution key, clustering key, primary key, storage format,
   * orientation, etc.
   */
  @Override
  protected java.util.Map<String, String> getTableProperties(
      java.sql.Connection connection, String tableName) throws SQLException {
    java.util.Map<String, String> properties = new java.util.HashMap<>();

    // Query Hologres table properties
    String query =
        "SELECT property_key, property_value FROM hologres.hg_table_properties "
            + "WHERE table_namespace = ? AND table_name = ?";

    try (java.sql.PreparedStatement stmt = connection.prepareStatement(query)) {
      stmt.setString(1, connection.getSchema());
      stmt.setString(2, tableName);

      try (java.sql.ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          String key = rs.getString("property_key");
          String value = rs.getString("property_value");
          if (key != null && value != null) {
            // Prefix Hologres-specific properties to avoid conflicts
            properties.put("hologres." + key, value);
          }
        }
      }
    } catch (SQLException e) {
      // If the query fails (e.g., permission issue or table doesn't exist), log and continue
      LOG.warn(
          "Failed to query Hologres table properties for table {}: {}", tableName, e.getMessage());
    }

    return properties;
  }

  @Override
  protected JdbcTable.Builder getTableBuilder(
      java.sql.ResultSet tablesResult, String databaseName, String tableName) throws SQLException {
    JdbcTable.Builder builder = super.getTableBuilder(tablesResult, databaseName, tableName);
    return builder;
  }

  @Override
  protected JdbcColumn.Builder getColumnBuilder(
      java.sql.ResultSet columnsResult, String databaseName, String tableName) throws SQLException {
    JdbcColumn.Builder builder = super.getColumnBuilder(columnsResult, databaseName, tableName);
    return builder;
  }

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes) {
    if (partitioning != null && partitioning.length > 0) {
      throw new UnsupportedOperationException(
          "Currently we do not support Partitioning in Hologres");
    }

    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder
        .append("CREATE TABLE ")
        .append(HOLO_QUOTE)
        .append(tableName)
        .append(HOLO_QUOTE)
        .append(" (")
        .append(NEW_LINE);

    // Add columns
    for (int i = 0; i < columns.length; i++) {
      JdbcColumn column = columns[i];
      sqlBuilder.append("    ").append(HOLO_QUOTE).append(column.name()).append(HOLO_QUOTE);
      sqlBuilder.append(" ").append(column.dataType());
      if (column.nullable()) {
        sqlBuilder.append(" NULL");
      } else {
        sqlBuilder.append(" NOT NULL");
      }
      if (column.autoIncrement()) {
        // Hologres may support different auto-increment syntax
        sqlBuilder.append(" GENERATED BY DEFAULT AS IDENTITY");
      }
      // Add a comma for the next column, unless it's the last one
      if (i < columns.length - 1) {
        sqlBuilder.append(",").append(NEW_LINE);
      }
    }

    sqlBuilder.append(NEW_LINE).append(")");

    // Add table comment if specified
    if (StringUtils.isNotEmpty(comment)) {
      sqlBuilder
          .append(";")
          .append(NEW_LINE)
          .append(TABLE_COMMENT)
          .append(HOLO_QUOTE)
          .append(tableName)
          .append(HOLO_QUOTE)
          .append(IS)
          .append(comment)
          .append("';");
    }

    // Add column comments
    for (JdbcColumn column : columns) {
      if (StringUtils.isNotEmpty(column.comment())) {
        sqlBuilder
            .append(NEW_LINE)
            .append(COLUMN_COMMENT)
            .append(HOLO_QUOTE)
            .append(tableName)
            .append(HOLO_QUOTE)
            .append(".")
            .append(HOLO_QUOTE)
            .append(column.name())
            .append(HOLO_QUOTE)
            .append(IS)
            .append(column.comment())
            .append("';");
      }
    }

    String result = sqlBuilder.toString();
    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw new UnsupportedOperationException(
        "Hologres does not support purge table in Gravitino, please use drop table");
  }

  @Override
  protected String generateAlterTableSql(
      String schemaName, String tableName, TableChange... changes) {
    // Hologres follows PostgreSQL ALTER TABLE syntax
    // For now, delegate to the parent class implementation
    // This may need to be customized for Hologres-specific features
    throw new UnsupportedOperationException(
        "Alter table operations are not yet fully implemented for Hologres");
  }
}
