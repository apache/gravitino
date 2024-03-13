/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system;

import static com.datastrato.gravitino.trino.connector.system.table.GravitinoSystemTable.SYSTEM_TABLE_SCHEMA_NAME;

import com.datastrato.gravitino.trino.connector.system.table.GravitinoSystemTableFactory;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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

/** An implementation of System connector Metadata * */
public class GravitinoSystemConnectorMetadata implements ConnectorMetadata {

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    return List.of(SYSTEM_TABLE_SCHEMA_NAME);
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
    return GravitinoSystemTableFactory.systemTableDefines.keySet().stream().toList();
  }

  @Override
  public ConnectorTableHandle getTableHandle(
      ConnectorSession session,
      SchemaTableName tableName,
      Optional<ConnectorTableVersion> startVersion,
      Optional<ConnectorTableVersion> endVersion) {
    return GravitinoSystemTableFactory.systemTableDefines.get(tableName) != null
        ? new SystemTableHandle(tableName)
        : null;
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(
      ConnectorSession session, ConnectorTableHandle table) {
    SchemaTableName tableName = ((SystemTableHandle) table).name;
    return GravitinoSystemTableFactory.getTableMetaData(tableName);
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    SchemaTableName tableName = ((SystemTableHandle) tableHandle).name;
    Map<String, ColumnHandle> columnHandles = new HashMap<>();
    List<ColumnMetadata> columns =
        GravitinoSystemTableFactory.getTableMetaData(tableName).getColumns();
    for (int i = 0; i < columns.size(); i++) {
      columnHandles.put(columns.get(i).getName(), new SystemColumnHandle(i));
    }
    return columnHandles;
  }

  @Override
  public ColumnMetadata getColumnMetadata(
      ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
    SchemaTableName tableName = ((SystemTableHandle) tableHandle).name;
    return GravitinoSystemTableFactory.getTableMetaData(tableName)
        .getColumns()
        .get(((SystemColumnHandle) columnHandle).index);
  }

  public static class SystemTableHandle implements ConnectorTableHandle {
    SchemaTableName name;

    @JsonCreator
    public SystemTableHandle(@JsonProperty("name") SchemaTableName name) {
      this.name = name;
    }

    @JsonProperty
    public SchemaTableName getName() {
      return name;
    }
  }

  public static class SystemColumnHandle implements ColumnHandle {
    int index;

    @JsonCreator
    public SystemColumnHandle(@JsonProperty("index") int index) {
      this.index = index;
    }

    @JsonProperty
    public int getIndex() {
      return index;
    }
  }
}
