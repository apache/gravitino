/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.operation;

import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class SqliteTableOperations extends JdbcTableOperations {

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Index[] indexes) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("CREATE TABLE ").append(tableName).append(" (");

    for (JdbcColumn column : columns) {
      sqlBuilder
          .append(column.name())
          .append(" ")
          .append(typeConverter.fromGravitinoType(column.dataType()));
      if (!column.nullable()) {
        sqlBuilder.append(" NOT NULL");
      }

      if (!Column.DEFAULT_VALUE_NOT_SET.equals(column.defaultValue())) {
        sqlBuilder.append(" DEFAULT ");
        sqlBuilder.append(columnDefaultValueConverter.fromGravitino(column.defaultValue()));
      }

      sqlBuilder.append(",");
    }

    if (columns.length > 0) {
      sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
    }

    if (comment != null && !comment.isEmpty()) {
      sqlBuilder.append(") COMMENT '").append(comment).append("'");
    } else {
      sqlBuilder.append(")");
      if (properties != null && !properties.isEmpty()) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          sqlBuilder.append(" ").append(entry.getKey()).append("=").append(entry.getValue());
        }
      }
      sqlBuilder.append(";");
    }
    return sqlBuilder.toString();
  }

  @Override
  protected String generateRenameTableSql(String oldTableName, String newTableName) {
    return "ALTER TABLE " + oldTableName + " RENAME TO " + newTableName + ";";
  }

  @Override
  protected String generateDropTableSql(String tableName) {
    return "DROP TABLE " + tableName + ";";
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw new UnsupportedOperationException("Purge table is not supported in sqlite.");
  }

  @Override
  protected String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes) {
    throw new UnsupportedOperationException("Alter table is not supported in sqlite.");
  }

  @Override
  protected JdbcTable getOrCreateTable(
      String databaseName, String tableName, JdbcTable lazyLoadCreateTable) {
    throw new UnsupportedOperationException("Sqlite does not support lazy load create table.");
  }

  @Override
  protected boolean getAutoIncrementInfo(ResultSet columns) {
    return false;
  }

  @Override
  public List<String> listTables(String databaseName) throws NoSuchSchemaException {
    try (Connection connection = getConnection(databaseName)) {
      final List<String> names = Lists.newArrayList();
      try (ResultSet tables = getTables(connection)) {
        // tables.getString("TABLE_SCHEM") is always null.
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
}
