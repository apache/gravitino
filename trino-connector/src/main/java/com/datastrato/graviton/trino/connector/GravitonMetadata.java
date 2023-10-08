/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_CATALOG_NOT_EXISTS;
import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_SCHEMA_NOT_EXISTS;
import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_TABLE_NOT_EXISTS;
import static java.util.Objects.requireNonNull;

import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NoSuchTableException;
import com.datastrato.graviton.trino.connector.catalog.CatalogConnectorMetadata;
import com.datastrato.graviton.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.graviton.trino.connector.metadata.GravitonColumn;
import com.datastrato.graviton.trino.connector.metadata.GravitonSchema;
import com.datastrato.graviton.trino.connector.metadata.GravitonTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The GravitonMetadata class provides operations for Graviton metadata on the Graviton server. It
 * also transforms the different metadata formats between Trino and Graviton. Additionally, it wraps
 * the internal connector metadata for accessing data.
 */
public class GravitonMetadata implements ConnectorMetadata {
  // handing metadata operations on graviton server
  private final CatalogConnectorMetadata catalogConnectorMetadata;

  // transform different metadata format
  private final CatalogConnectorMetadataAdapter metadataAdapter;

  private final ConnectorMetadata internalMetadata;

  public GravitonMetadata(
      CatalogConnectorMetadata catalogConnectorMetadata,
      CatalogConnectorMetadataAdapter metaDataAdapter,
      ConnectorMetadata internalMetadata) {
    this.catalogConnectorMetadata = catalogConnectorMetadata;
    this.metadataAdapter = metaDataAdapter;
    this.internalMetadata = internalMetadata;
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    try {
      return catalogConnectorMetadata.listSchemaNames();
    } catch (NoSuchCatalogException noSuchCatalogException) {
      throw new TrinoException(GRAVITON_CATALOG_NOT_EXISTS, noSuchCatalogException);
    }
  }

  @Override
  public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName) {
    try {
      GravitonSchema schema = catalogConnectorMetadata.getSchema(schemaName);
      return metadataAdapter.getSchemaProperties(schema);
    } catch (NoSuchSchemaException noSuchSchemaException) {
      throw new TrinoException(GRAVITON_SCHEMA_NOT_EXISTS, noSuchSchemaException);
    }
  }

  @Override
  public ConnectorTableProperties getTableProperties(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    try {
      GravitonTableHandle gravitonTableHandle = (GravitonTableHandle) tableHandle;
      GravitonTable table =
          catalogConnectorMetadata.getTable(
              gravitonTableHandle.getSchemaName(), gravitonTableHandle.getTableName());
      return metadataAdapter.getTableProperties(table);
    } catch (NoSuchTableException noSuchTableException) {
      throw new TrinoException(GRAVITON_TABLE_NOT_EXISTS, noSuchTableException);
    }
  }

  @Override
  public GravitonTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
    boolean tableExists =
        catalogConnectorMetadata.tableExists(tableName.getSchemaName(), tableName.getTableName());
    if (!tableExists) {
      throw new TrinoException(GRAVITON_TABLE_NOT_EXISTS, "Table does not exist");
    }

    ConnectorTableHandle internalTableHandle = internalMetadata.getTableHandle(session, tableName);
    return new GravitonTableHandle(
        tableName.getSchemaName(), tableName.getTableName(), internalTableHandle);
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    GravitonTableHandle gravitonTableHandle = (GravitonTableHandle) tableHandle;
    try {
      GravitonTable table =
          catalogConnectorMetadata.getTable(
              gravitonTableHandle.getSchemaName(), gravitonTableHandle.getTableName());
      return metadataAdapter.getTableMetaData(table);

    } catch (NoSuchTableException noSuchTableException) {
      throw new TrinoException(GRAVITON_TABLE_NOT_EXISTS, noSuchTableException);
    }
  }

  @Override
  public List<SchemaTableName> listTables(
      ConnectorSession session, Optional<String> optionalSchemaName) {
    Set<String> schemaNames =
        optionalSchemaName
            .map(ImmutableSet::of)
            .orElseGet(() -> ImmutableSet.copyOf(listSchemaNames(session)));

    try {
      ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
      for (String schemaName : schemaNames) {
        List<String> tableNames = catalogConnectorMetadata.listTables(schemaName);
        for (String tableName : tableNames) {
          builder.add(new SchemaTableName(schemaName, tableName));
        }
      }
      return builder.build();
    } catch (NoSuchSchemaException noSuchSchemaException) {
      throw new TrinoException(GRAVITON_SCHEMA_NOT_EXISTS, noSuchSchemaException);
    }
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    GravitonTableHandle gravitonTableHandle = (GravitonTableHandle) tableHandle;

    GravitonTable table =
        catalogConnectorMetadata.getTable(
            gravitonTableHandle.getSchemaName(), gravitonTableHandle.getTableName());

    if (table == null) {
      throw new TrinoException(GRAVITON_TABLE_NOT_EXISTS, "Table does not exist");
    }

    ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

    Map<String, ColumnHandle> internalColumnHandles =
        internalMetadata.getColumnHandles(session, gravitonTableHandle.getInternalTableHandle());
    for (GravitonColumn column : table.getColumns()) {
      GravitonColumnHandle columnHandle =
          new GravitonColumnHandle(column.getName(), internalColumnHandles.get(column.getName()));
      columnHandles.put(column.getName(), columnHandle);
    }
    return columnHandles.buildOrThrow();
  }

  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
      ConnectorSession session, SchemaTablePrefix prefix) {
    requireNonNull(prefix, "prefix is null");
    ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
    for (SchemaTableName tableName : listTables(session, prefix)) {
      GravitonTable table =
          catalogConnectorMetadata.getTable(tableName.getSchemaName(), tableName.getTableName());
      if (table == null) {
        throw new TrinoException(GRAVITON_TABLE_NOT_EXISTS, "Table does not exist");
      }

      ArrayList<ColumnMetadata> columnMetadataList = new ArrayList<>();
      for (GravitonColumn column : table.getColumns()) {
        ColumnMetadata columnMetadata = metadataAdapter.getColumnMetadata(column);
        columnMetadataList.add(columnMetadata);
      }
      columns.put(tableName, columnMetadataList);
    }
    return columns.buildOrThrow();
  }

  private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
    if (prefix.getTable().isEmpty()) {
      return listTables(session, prefix.getSchema());
    }
    return ImmutableList.of(prefix.toSchemaTableName());
  }

  @Override
  public ColumnMetadata getColumnMetadata(
      ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
    GravitonTableHandle gravitonTableHandle = (GravitonTableHandle) tableHandle;
    GravitonTable table =
        catalogConnectorMetadata.getTable(
            gravitonTableHandle.getSchemaName(), gravitonTableHandle.getTableName());

    if (table == null) {
      throw new TrinoException(GRAVITON_TABLE_NOT_EXISTS, "Table does not exist");
    }

    GravitonColumnHandle gravitonColumnHandle = (GravitonColumnHandle) columnHandle;
    String columName = gravitonColumnHandle.getColumnName();
    table.getColumn(columName);

    GravitonColumn column = table.getColumn(columName);
    return metadataAdapter.getColumnMetadata(column);
  }
}
