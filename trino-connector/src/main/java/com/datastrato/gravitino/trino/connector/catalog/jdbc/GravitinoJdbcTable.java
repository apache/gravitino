/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc;

import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql.MySQLPropertyMeta;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoColumn;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoTable;
import java.util.List;
import java.util.Map;

public class GravitinoJdbcTable extends GravitinoTable {
  public GravitinoJdbcTable(String schemaName, String tableName, Table tableMetadata) {
    super(schemaName, tableName, tableMetadata);
  }

  public GravitinoJdbcTable(
      String schemaName,
      String tableName,
      List<GravitinoColumn> columns,
      String comment,
      Map<String, String> properties) {
    super(schemaName, tableName, columns, comment, properties);
  }

  public GravitinoJdbcTable(
      String schemaName,
      String tableName,
      Column[] columns,
      String comment,
      Map<String, String> properties) {
    super(schemaName, tableName, columns, comment, properties);
  }

  // Get autoIncrement value from property.
  public Column[] getRawColumns() {
    Column[] gravitinoColumns = new Column[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      GravitinoColumn column = columns.get(i);
      boolean isAutoIncrement =
          (boolean) column.getProperties().getOrDefault(MySQLPropertyMeta.AUTO_INCREMENT, false);

      gravitinoColumns[i] =
          Column.of(
              column.getName(),
              column.getType(),
              column.getComment(),
              column.isNullable(),
              isAutoIncrement,
              null);
    }
    return gravitinoColumns;
  }
}
