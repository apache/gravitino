/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_COLUMN_NOT_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_TABLE_NOT_EXISTS;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoColumn;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoSchema;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * The GravitinoMetadata class provides operations for Gravitino metadata on the Gravitino server.
 * It also transforms the different metadata formats between Trino and Gravitino. Additionally, it
 * wraps the internal connector metadata for accessing data.
 */
public class GravitinoMetadata implements ConnectorMetadata {
  // Handling metadata operations on gravitino server
  private final CatalogConnectorMetadata catalogConnectorMetadata;

  // Transform different metadata format
  private final CatalogConnectorMetadataAdapter metadataAdapter;

  private final ConnectorMetadata internalMetadata;

  public GravitinoMetadata(
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
    GravitinoSchema schema = catalogConnectorMetadata.getSchema(schemaName);
    return metadataAdapter.getSchemaProperties(schema);
  }

  @Override
  public GravitinoTableHandle getTableHandle(
      ConnectorSession session,
      SchemaTableName tableName,
      Optional<ConnectorTableVersion> startVersion,
      Optional<ConnectorTableVersion> endVersion) {
    boolean tableExists =
        catalogConnectorMetadata.tableExists(tableName.getSchemaName(), tableName.getTableName());
    if (!tableExists) return null;

    ConnectorTableHandle internalTableHandle =
        internalMetadata.getTableHandle(session, tableName, startVersion, endVersion);

    if (internalTableHandle == null) {
      throw new TrinoException(
          GRAVITINO_TABLE_NOT_EXISTS,
          String.format("Table %s does not exist in the internal connector", tableName));
    }
    return new GravitinoTableHandle(
        tableName.getSchemaName(), tableName.getTableName(), internalTableHandle);
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    if (!(tableHandle instanceof GravitinoTableHandle)) {
      return internalMetadata.getTableMetadata(session, tableHandle);
    }
    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) tableHandle;
    GravitinoTable table =
        catalogConnectorMetadata.getTable(
            gravitinoTableHandle.getSchemaName(), gravitinoTableHandle.getTableName());
    return metadataAdapter.getTableMetadata(table);
  }

  @Override
  public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table) {
    if (!(table instanceof GravitinoTableHandle)) {
      return internalMetadata.getTableName(session, table);
    }

    return ((GravitinoTableHandle) table).toSchemaTableName();
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
    if (!(tableHandle instanceof GravitinoTableHandle)) {
      return internalMetadata.getColumnHandles(session, tableHandle);
    }

    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) tableHandle;
    Map<String, ColumnHandle> internalColumnHandles =
        internalMetadata.getColumnHandles(session, gravitinoTableHandle.getInternalTableHandle());
    return internalColumnHandles;
  }

  @Override
  public ColumnMetadata getColumnMetadata(
      ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
    if (!(tableHandle instanceof GravitinoTableHandle)) {
      return internalMetadata.getColumnMetadata(session, tableHandle, columnHandle);
    }

    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) tableHandle;
    return internalMetadata.getColumnMetadata(
        session, gravitinoTableHandle.getInternalTableHandle(), columnHandle);
  }

  @Override
  public void createTable(
      ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting) {
    GravitinoTable table = metadataAdapter.createTable(tableMetadata);
    catalogConnectorMetadata.createTable(table);
  }

  @Override
  public void createSchema(
      ConnectorSession session,
      String schemaName,
      Map<String, Object> properties,
      TrinoPrincipal owner) {
    GravitinoSchema schema = metadataAdapter.createSchema(schemaName, properties);
    catalogConnectorMetadata.createSchema(schema);
  }

  @Override
  public void dropSchema(ConnectorSession session, String schemaName, boolean cascade) {
    catalogConnectorMetadata.dropSchema(schemaName, cascade);
  }

  @Override
  public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) tableHandle;
    catalogConnectorMetadata.dropTable(gravitinoTableHandle.toSchemaTableName());
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
    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) tableHandle;
    ConnectorInsertTableHandle insertTableHandle =
        internalMetadata.beginInsert(
            session, gravitinoTableHandle.getInternalTableHandle(), columns, retryMode);
    return new GravitinoInsertTableHandle(insertTableHandle);
  }

  @Override
  public Optional<ConnectorOutputMetadata> finishInsert(
      ConnectorSession session,
      ConnectorInsertTableHandle insertHandle,
      Collection<Slice> fragments,
      Collection<ComputedStatistics> computedStatistics) {

    GravitinoInsertTableHandle gravitinoInsertTableHandle =
        (GravitinoInsertTableHandle) insertHandle;
    return internalMetadata.finishInsert(
        session,
        gravitinoInsertTableHandle.getInternalInsertTableHandle(),
        fragments,
        computedStatistics);
  }

  @Override
  public void renameSchema(ConnectorSession session, String source, String target) {
    catalogConnectorMetadata.renameSchema(source, target);
  }

  @Override
  public void renameTable(
      ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName) {
    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) tableHandle;
    catalogConnectorMetadata.renameTable(gravitinoTableHandle.toSchemaTableName(), newTableName);
  }

  @Override
  public void setTableComment(
      ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment) {
    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) tableHandle;
    catalogConnectorMetadata.setTableComment(
        gravitinoTableHandle.toSchemaTableName(), comment.orElse(""));
  }

  @Override
  public void setTableProperties(
      ConnectorSession session,
      ConnectorTableHandle tableHandle,
      Map<String, Optional<Object>> properties) {
    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) tableHandle;
    Map<String, Object> resultMap =
        properties.entrySet().stream()
            .filter(e -> e.getValue().isPresent())
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
    Map<String, String> allProps = metadataAdapter.toGravitinoTableProperties(resultMap);
    catalogConnectorMetadata.setTableProperties(gravitinoTableHandle.toSchemaTableName(), allProps);
  }

  @Override
  public void addColumn(
      ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column) {
    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) tableHandle;
    GravitinoColumn gravitinoColumn = metadataAdapter.createColumn(column);
    catalogConnectorMetadata.addColumn(gravitinoTableHandle.toSchemaTableName(), gravitinoColumn);
  }

  @Override
  public void dropColumn(
      ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column) {
    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) tableHandle;
    String columnName = getColumnName(session, gravitinoTableHandle, column);
    catalogConnectorMetadata.dropColumn(gravitinoTableHandle.toSchemaTableName(), columnName);
  }

  @Override
  public void renameColumn(
      ConnectorSession session,
      ConnectorTableHandle tableHandle,
      ColumnHandle source,
      String target) {
    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) tableHandle;
    String columnName = getColumnName(session, gravitinoTableHandle, source);
    catalogConnectorMetadata.renameColumn(
        gravitinoTableHandle.toSchemaTableName(), columnName, target);
  }

  @Override
  public void setColumnType(
      ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Type type) {
    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) tableHandle;
    String columnName = getColumnName(session, gravitinoTableHandle, column);
    catalogConnectorMetadata.setColumnType(
        gravitinoTableHandle.toSchemaTableName(),
        columnName,
        metadataAdapter.getDataTypeTransformer().getGravitinoType(type));
  }

  @Override
  public void setColumnComment(
      ConnectorSession session,
      ConnectorTableHandle tableHandle,
      ColumnHandle column,
      Optional<String> comment) {
    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) tableHandle;
    String columnName = getColumnName(session, gravitinoTableHandle, column);

    String commentString = "";
    if (comment.isPresent() && !StringUtils.isBlank(comment.get())) {
      commentString = comment.get();
    }
    catalogConnectorMetadata.setColumnComment(
        gravitinoTableHandle.toSchemaTableName(), columnName, commentString);
  }

  @Override
  public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(
      ConnectorSession session,
      JoinType joinType,
      ConnectorTableHandle left,
      ConnectorTableHandle right,
      ConnectorExpression joinCondition,
      Map<String, ColumnHandle> leftAssignments,
      Map<String, ColumnHandle> rightAssignments,
      JoinStatistics statistics) {
    if (!(left instanceof GravitinoTableHandle) && !(right instanceof GravitinoTableHandle)) {
      return internalMetadata.applyJoin(
          session,
          joinType,
          left,
          right,
          joinCondition,
          leftAssignments,
          rightAssignments,
          statistics);
    }

    if (!(left instanceof GravitinoTableHandle) || !(right instanceof GravitinoTableHandle)) {
      return Optional.empty();
    }

    GravitinoTableHandle gravitinoLeftTableHandle = (GravitinoTableHandle) left;
    GravitinoTableHandle gravitinoRightTableHandle = (GravitinoTableHandle) right;

    return internalMetadata.applyJoin(
        session,
        joinType,
        gravitinoLeftTableHandle.getInternalTableHandle(),
        gravitinoRightTableHandle.getInternalTableHandle(),
        joinCondition,
        leftAssignments,
        rightAssignments,
        statistics);
  }

  @Override
  public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
      ConnectorSession session,
      ConnectorTableHandle handle,
      List<ConnectorExpression> projections,
      Map<String, ColumnHandle> assignments) {
    if (!(handle instanceof GravitinoTableHandle)) {
      return internalMetadata.applyProjection(session, handle, projections, assignments);
    }

    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) handle;
    return internalMetadata.applyProjection(
        session, gravitinoTableHandle.getInternalTableHandle(), projections, assignments);
  }

  @Override
  public ColumnHandle getMergeRowIdColumnHandle(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    return ConnectorMetadata.super.getMergeRowIdColumnHandle(session, tableHandle);
  }

  @Override
  public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
      ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
    if (!(handle instanceof GravitinoTableHandle)) {
      return internalMetadata.applyFilter(session, handle, constraint);
    }

    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) handle;
    return internalMetadata.applyFilter(
        session, gravitinoTableHandle.getInternalTableHandle(), constraint);
  }

  @Override
  public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
      ConnectorSession session,
      ConnectorTableHandle handle,
      List<AggregateFunction> aggregates,
      Map<String, ColumnHandle> assignments,
      List<List<ColumnHandle>> groupingSets) {
    if (!(handle instanceof GravitinoTableHandle)) {
      return internalMetadata.applyAggregation(
          session, handle, aggregates, assignments, groupingSets);
    }

    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) handle;
    return internalMetadata.applyAggregation(
        session,
        gravitinoTableHandle.getInternalTableHandle(),
        aggregates,
        assignments,
        groupingSets);
  }

  @Override
  public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
      ConnectorSession session, ConnectorTableHandle handle, long limit) {
    if (!(handle instanceof GravitinoTableHandle)) {
      return internalMetadata.applyLimit(session, handle, limit);
    }

    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) handle;
    return internalMetadata.applyLimit(
        session, gravitinoTableHandle.getInternalTableHandle(), limit);
  }

  @Override
  public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
      ConnectorSession session,
      ConnectorTableHandle handle,
      long topNCount,
      List<SortItem> sortItems,
      Map<String, ColumnHandle> assignments) {
    if (!(handle instanceof GravitinoTableHandle)) {
      return internalMetadata.applyTopN(session, handle, topNCount, sortItems, assignments);
    }

    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) handle;
    return internalMetadata.applyTopN(
        session, gravitinoTableHandle.getInternalTableHandle(), topNCount, sortItems, assignments);
  }

  @Override
  public TableStatistics getTableStatistics(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    if (!(tableHandle instanceof GravitinoTableHandle)) {
      return internalMetadata.getTableStatistics(session, tableHandle);
    }

    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) tableHandle;
    return internalMetadata.getTableStatistics(
        session, gravitinoTableHandle.getInternalTableHandle());
  }

  private String getColumnName(
      ConnectorSession session, GravitinoTableHandle tableHandle, ColumnHandle columnHandle) {
    ColumnMetadata internalMetadataColumnMetadata =
        internalMetadata.getColumnMetadata(
            session, tableHandle.getInternalTableHandle(), columnHandle);
    if (internalMetadataColumnMetadata == null) {
      throw new TrinoException(
          GRAVITINO_COLUMN_NOT_EXISTS,
          String.format("Column %s does not exist in the internal connector", columnHandle));
    }
    return internalMetadataColumnMetadata.getName();
  }
}
