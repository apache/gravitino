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
package org.apache.gravitino.trino.connector.system;

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
import org.apache.gravitino.trino.connector.system.table.GravitinoSystemTable;
import org.apache.gravitino.trino.connector.system.table.GravitinoSystemTableFactory;

/** An implementation of Apache Gravitino System connector Metadata */
public class GravitinoSystemConnectorMetadata implements ConnectorMetadata {

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    return List.of(GravitinoSystemTable.SYSTEM_TABLE_SCHEMA_NAME);
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
    return GravitinoSystemTableFactory.SYSTEM_TABLES.keySet().stream().toList();
  }

  @Override
  public ConnectorTableHandle getTableHandle(
      ConnectorSession session,
      SchemaTableName tableName,
      Optional<ConnectorTableVersion> startVersion,
      Optional<ConnectorTableVersion> endVersion) {
    return GravitinoSystemTableFactory.SYSTEM_TABLES.get(tableName) != null
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

  /** A handle for system tables in the Gravitino connector. */
  public static class SystemTableHandle implements ConnectorTableHandle {
    private final SchemaTableName name;

    /**
     * Constructs a new SystemTableHandle.
     *
     * @param name the schema-qualified table name
     */
    @JsonCreator
    public SystemTableHandle(@JsonProperty("name") SchemaTableName name) {
      this.name = name;
    }

    /**
     * Gets the schema-qualified table name.
     *
     * @return the table name
     */
    @JsonProperty
    public SchemaTableName getName() {
      return name;
    }
  }

  /** A handle for columns in system tables. */
  public static class SystemColumnHandle implements ColumnHandle {
    private final int index;

    /**
     * Constructs a new SystemColumnHandle.
     *
     * @param index the index of the column in the table
     */
    @JsonCreator
    public SystemColumnHandle(@JsonProperty("index") int index) {
      this.index = index;
    }

    /**
     * Gets the index of the column.
     *
     * @return the column index
     */
    @JsonProperty
    public int getIndex() {
      return index;
    }
  }
}
