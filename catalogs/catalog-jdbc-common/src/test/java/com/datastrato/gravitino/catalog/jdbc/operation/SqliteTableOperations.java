/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.operation;

import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Map;

public class SqliteTableOperations extends JdbcTableOperations {

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning) {
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
  protected String generateAlterTableSql(String tableName, TableChange... changes) {
    throw new UnsupportedOperationException("Alter table is not supported in sqlite.");
  }

  @Override
  protected Map<String, String> extractPropertiesFromResultSet(ResultSet table) {
    // Sqlite does not support table properties.
    return Collections.emptyMap();
  }
}
