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
import io.trino.spi.connector.Assignment;
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
import io.trino.spi.connector.SaveMode;
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
    GravitinoTableHandle gravitinoTableHandle = (GravitinoTableHandle) tableHandle;
    GravitinoTable table =
        catalogConnectorMetadata.getTable(
            gravitinoTableHandle.getSchemaName(), gravitinoTableHandle.getTableName());
    return metadataAdapter.getTableMetadata(table);
  }

  @Override
  public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table) {
    return getTableName(table);
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
    Map<String, ColumnHandle> internalColumnHandles =
        internalMetadata.getColumnHandles(session, GravitinoHandle.unWrap(tableHandle));
    return internalColumnHandles.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                (entry) -> new GravitinoColumnHandle(entry.getKey(), entry.getValue())));
  }

  @Override
  public ColumnMetadata getColumnMetadata(
      ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
    return internalMetadata.getColumnMetadata(
        session, GravitinoHandle.unWrap(tableHandle), GravitinoHandle.unWrap(columnHandle));
  }

  @Override
  public void createTable(
      ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode) {
    GravitinoTable table = metadataAdapter.createTable(tableMetadata);
    // saveMode = SaveMode.IGNORE is used to ignore the table creation if it already exists
    catalogConnectorMetadata.createTable(table, saveMode == SaveMode.IGNORE);
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
    catalogConnectorMetadata.dropTable(getTableName(tableHandle));
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
    ConnectorInsertTableHandle insertTableHandle =
        internalMetadata.beginInsert(
            session,
            GravitinoHandle.unWrap(tableHandle),
            GravitinoHandle.unWrap(columns),
            retryMode);
    return new GravitinoInsertTableHandle(insertTableHandle);
  }

  @Override
  public Optional<ConnectorOutputMetadata> finishInsert(
      ConnectorSession session,
      ConnectorInsertTableHandle insertHandle,
      Collection<Slice> fragments,
      Collection<ComputedStatistics> computedStatistics) {
    return internalMetadata.finishInsert(
        session, GravitinoHandle.unWrap(insertHandle), fragments, computedStatistics);
  }

  @Override
  public void renameSchema(ConnectorSession session, String source, String target) {
    catalogConnectorMetadata.renameSchema(source, target);
  }

  @Override
  public void renameTable(
      ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName) {
    catalogConnectorMetadata.renameTable(getTableName(tableHandle), newTableName);
  }

  @Override
  public void setTableComment(
      ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment) {
    catalogConnectorMetadata.setTableComment(getTableName(tableHandle), comment.orElse(""));
  }

  @Override
  public void setTableProperties(
      ConnectorSession session,
      ConnectorTableHandle tableHandle,
      Map<String, Optional<Object>> properties) {
    Map<String, Object> resultMap =
        properties.entrySet().stream()
            .filter(e -> e.getValue().isPresent())
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
    Map<String, String> allProps = metadataAdapter.toGravitinoTableProperties(resultMap);
    catalogConnectorMetadata.setTableProperties(getTableName(tableHandle), allProps);
  }

  @Override
  public void addColumn(
      ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column) {
    GravitinoColumn gravitinoColumn = metadataAdapter.createColumn(column);
    catalogConnectorMetadata.addColumn(getTableName(tableHandle), gravitinoColumn);
  }

  @Override
  public void dropColumn(
      ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column) {
    String columnName = getColumnName(column);
    catalogConnectorMetadata.dropColumn(getTableName(tableHandle), columnName);
  }

  @Override
  public void renameColumn(
      ConnectorSession session,
      ConnectorTableHandle tableHandle,
      ColumnHandle source,
      String target) {
    String columnName = getColumnName(source);
    catalogConnectorMetadata.renameColumn(getTableName(tableHandle), columnName, target);
  }

  @Override
  public void setColumnType(
      ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Type type) {
    String columnName = getColumnName(column);
    catalogConnectorMetadata.setColumnType(
        getTableName(tableHandle),
        columnName,
        metadataAdapter.getDataTypeTransformer().getGravitinoType(type));
  }

  @Override
  public void setColumnComment(
      ConnectorSession session,
      ConnectorTableHandle tableHandle,
      ColumnHandle column,
      Optional<String> comment) {
    String columnName = getColumnName(column);

    String commentString = "";
    if (comment.isPresent() && !StringUtils.isBlank(comment.get())) {
      commentString = comment.get();
    }
    catalogConnectorMetadata.setColumnComment(getTableName(tableHandle), columnName, commentString);
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
    return internalMetadata
        .applyJoin(
            session,
            joinType,
            GravitinoHandle.unWrap(left),
            GravitinoHandle.unWrap(right),
            joinCondition,
            leftAssignments.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey, entry -> GravitinoHandle.unWrap(entry.getValue()))),
            rightAssignments.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey, entry -> GravitinoHandle.unWrap(entry.getValue()))),
            statistics)
        .map(
            result ->
                new JoinApplicationResult<>(
                    new GravitinoTableHandle(
                        getTableName(left).getSchemaName(),
                        getTableName(left).getTableName(),
                        result.getTableHandle()),
                    result.getLeftColumnHandles().entrySet().stream()
                        .collect(
                            Collectors.toMap(
                                entry ->
                                    new GravitinoColumnHandle(
                                        getColumnName(
                                            session, GravitinoHandle.unWrap(left), entry.getKey()),
                                        entry.getKey()),
                                entry ->
                                    new GravitinoColumnHandle(
                                        getColumnName(
                                            session,
                                            GravitinoHandle.unWrap(left),
                                            entry.getValue()),
                                        entry.getValue()))),
                    result.getRightColumnHandles().entrySet().stream()
                        .collect(
                            Collectors.toMap(
                                entry ->
                                    new GravitinoColumnHandle(
                                        getColumnName(
                                            session, GravitinoHandle.unWrap(right), entry.getKey()),
                                        entry.getKey()),
                                entry ->
                                    new GravitinoColumnHandle(
                                        getColumnName(
                                            session,
                                            GravitinoHandle.unWrap(right),
                                            entry.getValue()),
                                        entry.getValue()))),
                    result.isPrecalculateStatistics()));
  }

  @Override
  public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
      ConnectorSession session,
      ConnectorTableHandle handle,
      List<ConnectorExpression> projections,
      Map<String, ColumnHandle> assignments) {
    return internalMetadata
        .applyProjection(
            session,
            GravitinoHandle.unWrap(handle),
            projections,
            assignments.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey, entry -> GravitinoHandle.unWrap(entry.getValue()))))
        .map(
            result ->
                new ProjectionApplicationResult<>(
                    new GravitinoTableHandle(
                        getTableName(handle).getSchemaName(),
                        getTableName(handle).getTableName(),
                        result.getHandle()),
                    result.getProjections(),
                    result.getAssignments().stream()
                        .map(
                            entry ->
                                new Assignment(
                                    entry.getVariable(),
                                    new GravitinoColumnHandle(
                                        getColumnName(
                                            session,
                                            GravitinoHandle.unWrap(handle),
                                            entry.getColumn()),
                                        entry.getColumn()),
                                    entry.getType()))
                        .toList(),
                    result.isPrecalculateStatistics()));
  }

  @Override
  public ColumnHandle getMergeRowIdColumnHandle(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    return ConnectorMetadata.super.getMergeRowIdColumnHandle(session, tableHandle);
  }

  @Override
  public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
      ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint) {
    return internalMetadata
        .applyFilter(
            session, GravitinoHandle.unWrap(tableHandle), new GravitinoConstraint(constraint))
        .map(
            result ->
                new ConstraintApplicationResult<ConnectorTableHandle>(
                    new GravitinoTableHandle(
                        getTableName(tableHandle).getSchemaName(),
                        getTableName(tableHandle).getTableName(),
                        result.getHandle()),
                    result
                        .getRemainingFilter()
                        .transformKeys(
                            (columnHandle) ->
                                new GravitinoColumnHandle(
                                    getColumnName(
                                        session, GravitinoHandle.unWrap(tableHandle), columnHandle),
                                    columnHandle)),
                    result.getRemainingExpression().get(),
                    result.isPrecalculateStatistics()));
  }

  @Override
  public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
      ConnectorSession session,
      ConnectorTableHandle handle,
      List<AggregateFunction> aggregates,
      Map<String, ColumnHandle> assignments,
      List<List<ColumnHandle>> groupingSets) {
    return internalMetadata
        .applyAggregation(
            session,
            GravitinoHandle.unWrap(handle),
            aggregates,
            assignments.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey, entry -> GravitinoHandle.unWrap(entry.getValue()))),
            groupingSets.stream()
                .map(
                    innerList ->
                        innerList.stream()
                            .map(GravitinoHandle::unWrap)
                            .collect(Collectors.toList()))
                .collect(Collectors.toList()))
        .map(
            result ->
                new AggregationApplicationResult<ConnectorTableHandle>(
                    new GravitinoTableHandle(
                        getTableName(handle).getSchemaName(),
                        getTableName(handle).getTableName(),
                        result.getHandle()),
                    result.getProjections(),
                    result.getAssignments().stream()
                        .map(
                            entry ->
                                new Assignment(
                                    entry.getVariable(),
                                    new GravitinoColumnHandle(
                                        getColumnName(
                                            session,
                                            GravitinoHandle.unWrap(handle),
                                            entry.getColumn()),
                                        entry.getColumn()),
                                    entry.getType()))
                        .toList(),
                    result.getGroupingColumnMapping().entrySet().stream()
                        .collect(
                            Collectors.toMap(
                                entry ->
                                    new GravitinoColumnHandle(
                                        getColumnName(
                                            session,
                                            GravitinoHandle.unWrap(handle),
                                            entry.getKey()),
                                        entry.getKey()),
                                entry ->
                                    new GravitinoColumnHandle(
                                        getColumnName(
                                            session,
                                            GravitinoHandle.unWrap(handle),
                                            entry.getValue()),
                                        entry.getValue()))),
                    result.isPrecalculateStatistics()));
  }

  @Override
  public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
      ConnectorSession session, ConnectorTableHandle handle, long limit) {
    return internalMetadata
        .applyLimit(session, GravitinoHandle.unWrap(handle), limit)
        .map(
            result ->
                new LimitApplicationResult<ConnectorTableHandle>(
                    new GravitinoTableHandle(
                        getTableName(handle).getSchemaName(),
                        getTableName(handle).getTableName(),
                        result.getHandle()),
                    result.isLimitGuaranteed(),
                    result.isPrecalculateStatistics()));
  }

  @Override
  public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
      ConnectorSession session,
      ConnectorTableHandle handle,
      long topNCount,
      List<SortItem> sortItems,
      Map<String, ColumnHandle> assignments) {
    return internalMetadata
        .applyTopN(
            session,
            GravitinoHandle.unWrap(handle),
            topNCount,
            sortItems,
            assignments.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey, entry -> GravitinoHandle.unWrap(entry.getValue()))))
        .map(
            result ->
                new TopNApplicationResult<ConnectorTableHandle>(
                    new GravitinoTableHandle(
                        getTableName(handle).getSchemaName(),
                        getTableName(handle).getTableName(),
                        result.getHandle()),
                    result.isTopNGuaranteed(),
                    result.isPrecalculateStatistics()));
  }

  @Override
  public TableStatistics getTableStatistics(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    return internalMetadata.getTableStatistics(session, GravitinoHandle.unWrap(tableHandle));
  }

  private SchemaTableName getTableName(ConnectorTableHandle tableHandle) {
    return ((GravitinoTableHandle) tableHandle).toSchemaTableName();
  }

  private String getColumnName(ColumnHandle columnHandle) {
    return ((GravitinoColumnHandle) columnHandle).getColumnName();
  }

  private String getColumnName(
      ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
    ColumnMetadata internalMetadataColumnMetadata =
        internalMetadata.getColumnMetadata(session, tableHandle, columnHandle);
    if (internalMetadataColumnMetadata == null) {
      throw new TrinoException(
          GRAVITINO_COLUMN_NOT_EXISTS,
          String.format("Column %s does not exist in the internal connector", columnHandle));
    }
    return internalMetadataColumnMetadata.getName();
  }
}
