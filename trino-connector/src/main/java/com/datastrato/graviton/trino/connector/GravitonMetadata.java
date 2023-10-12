/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import com.datastrato.graviton.trino.connector.catalog.CatalogConnectorMetadata;
import com.datastrato.graviton.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.graviton.trino.connector.metadata.GravitonColumn;
import com.datastrato.graviton.trino.connector.metadata.GravitonSchema;
import com.datastrato.graviton.trino.connector.metadata.GravitonTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import java.util.ArrayList;
import java.util.Collection;
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
  // Handling metadata operations on graviton server
  private final CatalogConnectorMetadata catalogConnectorMetadata;

  // Transform different metadata format
  private final CatalogConnectorMetadataAdapter metadataAdapter;

  private final ConnectorMetadata internalMetadata;

  public GravitonMetadata(
      CatalogConnectorMetadata catalogConnectorMetadata,
      CatalogConnectorMetadataAdapter metadataAdapter,
      ConnectorMetadata internalMetadata) {
    this.catalogConnectorMetadata = catalogConnectorMetadata;
    this.metadataAdapter = metadataAdapter;
    this.internalMetadata = internalMetadata;
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    return catalogConnectorMetadata.listSchemaNames();
  }

  @Override
  public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName) {
    GravitonSchema schema = catalogConnectorMetadata.getSchema(schemaName);
    return metadataAdapter.getSchemaProperties(schema);
  }

  @Override
  public ConnectorTableProperties getTableProperties(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    GravitonTableHandle gravitonTableHandle = (GravitonTableHandle) tableHandle;
    GravitonTable table =
        catalogConnectorMetadata.getTable(
            gravitonTableHandle.getSchemaName(), gravitonTableHandle.getTableName());
    return metadataAdapter.getTableProperties(table);
  }

  @Override
  public GravitonTableHandle getTableHandle(
      ConnectorSession session,
      SchemaTableName tableName,
      Optional<ConnectorTableVersion> startVersion,
      Optional<ConnectorTableVersion> endVersion) {
    boolean tableExists =
        catalogConnectorMetadata.tableExists(tableName.getSchemaName(), tableName.getTableName());
    if (!tableExists) return null;

    ConnectorTableHandle internalTableHandle =
        internalMetadata.getTableHandle(session, tableName, startVersion, endVersion);
    return new GravitonTableHandle(
        tableName.getSchemaName(), tableName.getTableName(), internalTableHandle);
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    GravitonTableHandle gravitonTableHandle = (GravitonTableHandle) tableHandle;
    GravitonTable table =
        catalogConnectorMetadata.getTable(
            gravitonTableHandle.getSchemaName(), gravitonTableHandle.getTableName());
    return metadataAdapter.getTableMetadata(table);
  }

  @Override
  public List<SchemaTableName> listTables(
      ConnectorSession session, Optional<String> optionalSchemaName) {
    Set<String> schemaNames =
        optionalSchemaName
            .map(ImmutableSet::of)
            .orElseGet(() -> ImmutableSet.copyOf(listSchemaNames(session)));

    ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
    for (String schemaName : schemaNames) {
      List<String> tableNames = catalogConnectorMetadata.listTables(schemaName);
      for (String tableName : tableNames) {
        builder.add(new SchemaTableName(schemaName, tableName));
      }
    }
    return builder.build();
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    GravitonTableHandle gravitonTableHandle = (GravitonTableHandle) tableHandle;

    GravitonTable table =
        catalogConnectorMetadata.getTable(
            gravitonTableHandle.getSchemaName(), gravitonTableHandle.getTableName());

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

  @Override
  public ColumnMetadata getColumnMetadata(
      ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
    GravitonTableHandle gravitonTableHandle = (GravitonTableHandle) tableHandle;
    GravitonTable table =
        catalogConnectorMetadata.getTable(
            gravitonTableHandle.getSchemaName(), gravitonTableHandle.getTableName());

    GravitonColumnHandle gravitonColumnHandle = (GravitonColumnHandle) columnHandle;
    String columName = gravitonColumnHandle.getColumnName();

    GravitonColumn column = table.getColumn(columName);
    return metadataAdapter.getColumnMetadata(column);
  }

  @Override
  public void createTable(
      ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting) {
    GravitonTable table = metadataAdapter.createTable(tableMetadata);
    catalogConnectorMetadata.createTable(table);
  }

  @Override
  public void createSchema(
      ConnectorSession session,
      String schemaName,
      Map<String, Object> properties,
      TrinoPrincipal owner) {
    GravitonSchema schema = metadataAdapter.createSchema(schemaName, properties);
    catalogConnectorMetadata.createSchema(schema);
  }

  @Override
  public void dropSchema(ConnectorSession session, String schemaName, boolean cascade) {
    catalogConnectorMetadata.dropSchema(schemaName, cascade);
  }

  @Override
  public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
    GravitonTableHandle gravitonTableHandle = (GravitonTableHandle) tableHandle;
    catalogConnectorMetadata.dropTable(
        gravitonTableHandle.getTableName(), gravitonTableHandle.getTableName());
  }

  @Override
  public void beginQuery(ConnectorSession session) {
    internalMetadata.beginQuery(session);
  }

  @Override
  public void cleanupQuery(ConnectorSession session) {
    internalMetadata.cleanupQuery(session);
  }

  @Override
  public ConnectorInsertTableHandle beginInsert(
      ConnectorSession session,
      ConnectorTableHandle tableHandle,
      List<ColumnHandle> columns,
      RetryMode retryMode) {
    GravitonTableHandle gravitonTableHandle = (GravitonTableHandle) tableHandle;
    List<ColumnHandle> internalColumnHandles = new ArrayList<>();
    for (ColumnHandle column : columns) {
      internalColumnHandles.add(((GravitonColumnHandle) column).getInternalColumnHandler());
    }
    ConnectorInsertTableHandle insertTableHandle =
        internalMetadata.beginInsert(
            session,
            gravitonTableHandle.getInternalTableHandle(),
            internalColumnHandles,
            retryMode);
    return new GravitonInsertTableHandle(insertTableHandle);
  }

  @Override
  public Optional<ConnectorOutputMetadata> finishInsert(
      ConnectorSession session,
      ConnectorInsertTableHandle insertHandle,
      Collection<Slice> fragments,
      Collection<ComputedStatistics> computedStatistics) {

    GravitonInsertTableHandle gravitonInsertTableHandle = (GravitonInsertTableHandle) insertHandle;
    return internalMetadata.finishInsert(
        session,
        gravitonInsertTableHandle.getInternalInsertTableHandle(),
        fragments,
        computedStatistics);
  }
}
