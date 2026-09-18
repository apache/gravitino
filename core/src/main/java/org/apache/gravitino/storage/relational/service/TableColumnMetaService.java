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
package org.apache.gravitino.storage.relational.service;

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.po.ColumnPO;
import org.apache.gravitino.storage.relational.po.OwnerRelForDeletion;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

public class TableColumnMetaService {

  private static final int COLUMN_INSERT_BATCH_SIZE = 1000;
  private static final TableColumnMetaService INSTANCE = new TableColumnMetaService();

  private TableColumnMetaService() {}

  public static TableColumnMetaService getInstance() {
    return INSTANCE;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getColumnsByTableIdAndVersion")
  List<ColumnPO> getColumnsByTableIdAndVersion(Long tableId, Long version) {
    List<ColumnPO> columnPOs =
        SessionUtils.getWithoutCommit(
            TableColumnMapper.class,
            mapper -> mapper.listColumnPOsByTableIdAndVersion(tableId, version));

    // Filter out the deleted columns
    return columnPOs.stream()
        .filter(c -> c.getColumnOpType() != ColumnPO.ColumnOpType.DELETE.value())
        .collect(Collectors.toList());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getColumnIdByTableIdAndName")
  public Long getColumnIdByTableIdAndName(Long tableId, String columnName) {
    Long columnId =
        SessionUtils.getWithoutCommit(
            TableColumnMapper.class,
            mapper -> mapper.selectColumnIdByTableIdAndName(tableId, columnName));

    if (columnId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.COLUMN.name().toLowerCase(Locale.ROOT),
          columnName);
    }

    return columnId;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getColumnPOById")
  ColumnPO getColumnPOById(Long columnId) {
    ColumnPO columnPO =
        SessionUtils.getWithoutCommit(
            TableColumnMapper.class, mapper -> mapper.selectColumnPOById(columnId));

    if (columnPO == null || columnPO.getColumnOpType() == ColumnPO.ColumnOpType.DELETE.value()) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.COLUMN.name().toLowerCase(Locale.ROOT),
          columnId.toString());
    }

    return columnPO;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertColumnPOs")
  void insertColumnPOs(TablePO tablePO, List<ColumnEntity> columnEntities) {
    List<ColumnPO> columnPOs =
        POConverters.initializeColumnPOs(tablePO, columnEntities, ColumnPO.ColumnOpType.CREATE);

    insertColumnPOsInBatches(columnPOs);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteColumnsByTableId")
  boolean deleteColumnsByTableId(Long tableId) {
    // deleteColumns will be done in deleteTable transaction, so we don't do commit here.
    Integer result =
        SessionUtils.getWithoutCommit(
            TableColumnMapper.class, mapper -> mapper.softDeleteColumnsByTableId(tableId));
    return result > 0;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteColumnsByLegacyTimeline")
  public int deleteColumnsByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        TableColumnMapper.class,
        mapper -> mapper.deleteColumnPOsByLegacyTimeline(legacyTimeline, limit));
  }

  boolean isColumnUpdated(TableEntity oldTable, TableEntity newTable) {
    Map<Long, ColumnEntity> oldColumns =
        oldTable.columns() == null
            ? Collections.emptyMap()
            : oldTable.columns().stream()
                .collect(Collectors.toMap(ColumnEntity::id, Function.identity()));

    Map<Long, ColumnEntity> newColumns =
        newTable.columns() == null
            ? Collections.emptyMap()
            : newTable.columns().stream()
                .collect(Collectors.toMap(ColumnEntity::id, Function.identity()));

    return oldColumns.size() != newColumns.size() || !oldColumns.equals(newColumns);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateColumnPOsFromTableDiff")
  void updateColumnPOsFromTableDiff(
      TableEntity oldTable, TableEntity newTable, TablePO newTablePO) {
    Map<Long, ColumnEntity> oldColumns =
        oldTable.columns() == null
            ? Collections.emptyMap()
            : oldTable.columns().stream()
                .collect(Collectors.toMap(ColumnEntity::id, Function.identity()));
    Map<Long, ColumnEntity> newColumns =
        newTable.columns() == null
            ? Collections.emptyMap()
            : newTable.columns().stream()
                .collect(Collectors.toMap(ColumnEntity::id, Function.identity()));

    List<ColumnPO> columnPOsToInsert = Lists.newArrayList();
    for (ColumnEntity newColumn : newColumns.values()) {
      ColumnEntity oldColumn = oldColumns.get(newColumn.id());
      // If the column is not existed in old columns, or if the column is updated, mark it as UPDATE
      if (oldColumn == null || !oldColumn.equals(newColumn)) {
        columnPOsToInsert.add(
            POConverters.initializeColumnPO(newTablePO, newColumn, ColumnPO.ColumnOpType.UPDATE));
      }
    }

    // Mark the columns to DELETE if they are not existed in new columns.
    List<Long> deletedColumnIds = Lists.newArrayList();
    for (ColumnEntity oldColumn : oldColumns.values()) {
      if (!newColumns.containsKey(oldColumn.id())) {
        columnPOsToInsert.add(
            POConverters.initializeColumnPO(newTablePO, oldColumn, ColumnPO.ColumnOpType.DELETE));
        deletedColumnIds.add(oldColumn.id());
      }
    }

    // If the table moved to another schema, move all of its existing column rows as well, whether
    // or not any column changed. Only the changed columns get new rows below, so otherwise the
    // unchanged ones would stay under the old schema and be dropped with it.
    if (!newTable.namespace().equals(oldTable.namespace())) {
      SessionUtils.doWithoutCommit(
          TableColumnMapper.class,
          mapper ->
              mapper.updateSchemaIdByTableId(newTablePO.getTableId(), newTablePO.getSchemaId()));
    }

    // If there is no change, directly return
    if (columnPOsToInsert.isEmpty()) {
      return;
    }

    insertColumnPOsInBatches(columnPOsToInsert);
    deleteColumnRelations(deletedColumnIds);
  }

  private void deleteColumnRelations(List<Long> columnIds) {
    // A dropped column keeps its rows, so nothing else removes the relations that reference it.
    // This runs in the table update transaction, so the relations go away with the column. A wide
    // table can drop many columns at once, so the ids are deleted in batches. Policies cannot be
    // attached to columns, so there are no policy relations to remove.
    String columnType = MetadataObject.Type.COLUMN.name();
    Lists.partition(columnIds, COLUMN_INSERT_BATCH_SIZE)
        .forEach(
            batch -> {
              SessionUtils.doWithoutCommit(
                  TagMetadataObjectRelMapper.class,
                  mapper ->
                      mapper.softDeleteTagMetadataObjectRelsByMetadataObjects(batch, columnType));
              SessionUtils.doWithoutCommit(
                  OwnerMetaMapper.class,
                  mapper ->
                      mapper.batchSoftDeleteOwnerRelByMetadataObjects(
                          batch.stream()
                              .map(id -> new OwnerRelForDeletion(id, columnType))
                              .collect(Collectors.toList())));
            });
  }

  private void insertColumnPOsInBatches(List<ColumnPO> columnPOs) {
    // Column inserts run inside the table transaction, so no batch commits independently.
    Lists.partition(columnPOs, COLUMN_INSERT_BATCH_SIZE)
        .forEach(
            batch ->
                SessionUtils.doWithoutCommit(
                    TableColumnMapper.class, mapper -> mapper.insertColumnPOs(batch)));
  }
}
