/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.operation;

import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for managing tables in a JDBC data store. */
public abstract class JdbcTableOperations implements TableOperation {

  public static final String COMMENT = "COMMENT";
  public static final String SPACE = " ";

  protected static final Logger LOG = LoggerFactory.getLogger(JdbcTableOperations.class);

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
  public void create(
      String databaseName,
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Index[] indexes)
      throws TableAlreadyExistsException {
    LOG.info("Attempting to create table {} in database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      JdbcConnectorUtils.executeUpdate(
          connection,
          generateCreateTableSql(tableName, columns, comment, properties, partitioning, indexes));
      LOG.info("Created table {} in database {}", tableName, databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public void drop(String databaseName, String tableName) throws NoSuchTableException {
    LOG.info("Attempting to delete table {} from database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      JdbcConnectorUtils.executeUpdate(connection, generateDropTableSql(tableName));
      LOG.info("Deleted table {} from database {}", tableName, databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public List<String> listTables(String databaseName) throws NoSuchSchemaException {
    try (Connection connection = getConnection(databaseName)) {
      final List<String> names = Lists.newArrayList();
      try (ResultSet tables = getTables(connection)) {
        while (tables.next()) {
          names.add(tables.getString("TABLE_NAME"));
        }
      }
      LOG.info("Finished listing tables size {} for database name {} ", names.size(), databaseName);
      return names;
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public JdbcTable load(String databaseName, String tableName) throws NoSuchTableException {
    try (Connection connection = getConnection(databaseName)) {
      // 1.Get table information
      ResultSet table = getTable(connection, databaseName, tableName);
      if (!table.next() || !tableName.equals(table.getString("TABLE_NAME"))) {
        throw new NoSuchTableException(
            String.format("Table %s does not exist in %s.", tableName, databaseName));
      }
      JdbcTable.Builder jdbcTableBuilder = getBasicJdbcTableInfo(table);

      // 2.Get column information
      List<JdbcColumn> jdbcColumns = new ArrayList<>();
      ResultSet columns = getColumns(connection, databaseName, tableName);
      while (columns.next()) {
        JdbcColumn.Builder columnBuilder = getBasicJdbcColumnInfo(columns);
        boolean autoIncrement = getAutoIncrementInfo(columns);
        columnBuilder.withAutoIncrement(autoIncrement);
        jdbcColumns.add(columnBuilder.build());
      }
      jdbcTableBuilder.withColumns(jdbcColumns.toArray(new JdbcColumn[0]));

      // 3.Get index information
      List<Index> indexes = getIndexes(databaseName, tableName, connection.getMetaData());
      jdbcTableBuilder.withIndexes(indexes.toArray(new Index[0]));

      // 4.Get table properties
      Map<String, String> tableProperties = getTableProperties(connection, tableName);
      jdbcTableBuilder.withProperties(tableProperties);

      // 5.Leave the information to the bottom layer to append the table
      correctJdbcTableFields(connection, tableName, jdbcTableBuilder);
      return jdbcTableBuilder.build();
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
  }

  /**
   * Get all properties values of the table, including properties outside Gravitino management. The
   * JdbcCatalogOperations#loadTable method will filter out unnecessary properties.
   *
   * @param connection jdbc connection
   * @param tableName table name
   * @return Returns all table properties values.
   * @throws SQLException
   */
  protected Map<String, String> getTableProperties(Connection connection, String tableName)
      throws SQLException {
    return Collections.emptyMap();
  }

  protected boolean getAutoIncrementInfo(ResultSet resultSet) throws SQLException {
    return resultSet.getBoolean("IS_AUTOINCREMENT");
  }

  @Override
  public void rename(String databaseName, String oldTableName, String newTableName)
      throws NoSuchTableException {
    LOG.info(
        "Attempting to rename table {}/{} to {}/{}",
        databaseName,
        oldTableName,
        databaseName,
        newTableName);
    try (Connection connection = this.getConnection(databaseName)) {
      JdbcConnectorUtils.executeUpdate(
          connection, this.generateRenameTableSql(oldTableName, newTableName));
      LOG.info(
          "Renamed table {}/{} to {}/{}", databaseName, oldTableName, databaseName, newTableName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public void alterTable(String databaseName, String tableName, TableChange... changes)
      throws NoSuchTableException {
    LOG.info("Attempting to alter table {} from database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      String sql = generateAlterTableSql(databaseName, tableName, changes);
      if (StringUtils.isEmpty(sql)) {
        LOG.info("No changes to alter table {} from database {}", tableName, databaseName);
        return;
      }
      JdbcConnectorUtils.executeUpdate(connection, sql);
      LOG.info("Alter table {} from database {}", tableName, databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public void purge(String databaseName, String tableName) throws NoSuchTableException {
    LOG.info("Attempting to purge table {} from database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      JdbcConnectorUtils.executeUpdate(connection, generatePurgeTableSql(tableName));
      LOG.info("Purge table {} from database {}", tableName, databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  protected ResultSet getTables(Connection connection) throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    String databaseName = connection.getSchema();
    return metaData.getTables(databaseName, databaseName, null, JdbcConnectorUtils.TABLE_TYPES);
  }

  protected ResultSet getTable(Connection connection, String databaseName, String tableName)
      throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getTables(connection.getCatalog(), connection.getSchema(), tableName, null);
  }

  protected ResultSet getColumns(Connection connection, String databaseName, String tableName)
      throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getColumns(connection.getCatalog(), connection.getSchema(), tableName, null);
  }

  /**
   * Correct table information from the JDBC driver, because not all information could be retrieved
   * from the JDBC driver, like the table comment in MySQL of the 5.7 version.
   *
   * @param connection jdbc connection
   * @param tableName table name
   * @param jdbcTableBuilder The builder of the table to be returned
   * @throws SQLException
   */
  protected void correctJdbcTableFields(
      Connection connection, String tableName, JdbcTable.Builder jdbcTableBuilder)
      throws SQLException {
    // nothing to do
  }

  protected List<Index> getIndexes(String databaseName, String tableName, DatabaseMetaData metaData)
      throws SQLException {
    return Collections.emptyList();
  }

  protected abstract String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Index[] indexes);

  protected abstract String generateRenameTableSql(String oldTableName, String newTableName);

  protected abstract String generateDropTableSql(String tableName);

  protected abstract String generatePurgeTableSql(String tableName);

  protected abstract String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes);

  protected Connection getConnection(String catalog) throws SQLException {
    Connection connection = dataSource.getConnection();
    connection.setCatalog(catalog);
    return connection;
  }

  protected JdbcTable.Builder getBasicJdbcTableInfo(ResultSet table) throws SQLException {
    return new JdbcTable.Builder()
        .withName(table.getString("TABLE_NAME"))
        .withComment(table.getString("REMARKS"))
        .withAuditInfo(AuditInfo.EMPTY);
  }

  protected JdbcColumn.Builder getBasicJdbcColumnInfo(ResultSet column) throws SQLException {
    JdbcTypeConverter.JdbcTypeBean typeBean =
        new JdbcTypeConverter.JdbcTypeBean(column.getString("TYPE_NAME"));
    typeBean.setColumnSize(column.getString("COLUMN_SIZE"));
    typeBean.setScale(column.getString("DECIMAL_DIGITS"));
    String comment = column.getString("REMARKS");
    return new JdbcColumn.Builder()
        .withName(column.getString("COLUMN_NAME"))
        .withType(typeConverter.toGravitinoType(typeBean))
        .withComment(StringUtils.isEmpty(comment) ? null : comment)
        .withNullable(column.getBoolean("NULLABLE"))
        .withProperties(Collections.emptyList());
  }
}
