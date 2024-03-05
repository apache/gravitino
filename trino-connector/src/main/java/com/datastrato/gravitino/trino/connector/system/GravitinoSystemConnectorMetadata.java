/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class GravitinoSystemConnectorMetadata implements ConnectorMetadata {

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    return List.of("system");
  }

  @Override
  public ConnectorTableHandle getTableHandle(
      ConnectorSession session,
      SchemaTableName tableName,
      Optional<ConnectorTableVersion> startVersion,
      Optional<ConnectorTableVersion> endVersion) {
    return GravitinoSystemTable.systemTableDefines.get(tableName) != null
        ? new GravitinoSystemTableHandle(tableName)
        : null;
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(
      ConnectorSession session, ConnectorTableHandle table) {
    SchemaTableName tableName = ((GravitinoSystemTableHandle) table).name;
    return GravitinoSystemTable.getTableMetaData(tableName);
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    SchemaTableName tableName = ((GravitinoSystemTableHandle) tableHandle).name;
    Map<String, ColumnHandle> columnHandles = new HashMap<>();
    List<ColumnMetadata> columns = GravitinoSystemTable.getTableMetaData(tableName).getColumns();
    for (int i = 0; i < columns.size(); i++) {
      columnHandles.put(columns.get(i).getName(), new GravitinoSystemColumnHandle(i));
    }
    return columnHandles;
  }

  @Override
  public ColumnMetadata getColumnMetadata(
      ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
    SchemaTableName tableName = ((GravitinoSystemTableHandle) tableHandle).name;
    return GravitinoSystemTable.getTableMetaData(tableName).getColumns().get(((GravitinoSystemColumnHandle) columnHandle).index);
  }
}
