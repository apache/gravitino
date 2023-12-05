/*
 * Copyright 2023 Datastrato.
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
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for managing tables in a JDBC data store. */
public abstract class JdbcTableOperations implements TableOperation {

  protected static final Logger LOG = LoggerFactory.getLogger(JdbcTableOperations.class);

  protected DataSource dataSource;
  protected JdbcExceptionConverter exceptionMapper;
  protected JdbcTypeConverter typeConverter;

  @Override
  public void initialize(
      DataSource dataSource,
      JdbcExceptionConverter exceptionMapper,
      JdbcTypeConverter jdbcTypeConverter) {
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
      Transform[] partitioning)
      throws TableAlreadyExistsException {
    LOG.info("Attempting to create table {} in database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      JdbcConnectorUtils.executeUpdate(
          connection,
          generateCreateTableSql(tableName, columns, comment, properties, partitioning));
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
      String comment;
      Map<String, String> properties;
      try (ResultSet table = getTable(connection, tableName)) {
        if (!table.next()) {
          throw new NoSuchTableException("Table " + tableName + " does not exist.");
        }
        comment = table.getString("REMARKS");
        properties = extractPropertiesFromResultSet(table);
      }
      JdbcColumn[] jdbcColumns;
      try (ResultSet column = getColumns(connection, tableName)) {
        List<JdbcColumn> result = new ArrayList<>();
        while (column.next()) {
          result.add(extractJdbcColumnFromResultSet(column));
        }
        jdbcColumns = result.toArray(new JdbcColumn[0]);
      }
      return new JdbcTable.Builder()
          .withName(tableName)
          .withColumns(jdbcColumns)
          .withComment(comment)
          .withProperties(properties)
          .withAuditInfo(AuditInfo.EMPTY)
          .build();
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
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
      JdbcConnectorUtils.executeUpdate(
          connection, generateAlterTableSql(databaseName, tableName, changes));
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

  protected ResultSet getTable(Connection connection, String tableName) throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    String databaseName = connection.getSchema();
    return metaData.getTables(
        databaseName, databaseName, tableName, JdbcConnectorUtils.TABLE_TYPES);
  }

  protected ResultSet getColumns(Connection connection, String tableName) throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    String databaseName = connection.getSchema();
    return metaData.getColumns(databaseName, databaseName, tableName, null);
  }

  /**
   * @param table The result set of the table
   * @return The properties extracted from the result set
   */
  protected abstract Map<String, String> extractPropertiesFromResultSet(ResultSet table);

  protected abstract JdbcColumn extractJdbcColumnFromResultSet(ResultSet column)
      throws SQLException;

  protected abstract String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning);

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
}
