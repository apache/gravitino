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

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.po.ColumnPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

public class TableColumnMetaService {

  private static final TableColumnMetaService INSTANCE = new TableColumnMetaService();

  private TableColumnMetaService() {}

  public static TableColumnMetaService getInstance() {
    return INSTANCE;
  }

  public List<ColumnPO> getColumnsByTableIdAndVersion(Long tableId, Long version) {
    List<ColumnPO> columnPOs =
        SessionUtils.getWithoutCommit(
            TableColumnMapper.class,
            mapper -> mapper.listColumnPOsByTableIdAndVersion(tableId, version));

    // Filter out the deleted columns
    return columnPOs.stream()
        .filter(c -> !Objects.equals(c.getColumnOpType(), ColumnPO.ColumnOpType.DELETE.value()))
        .collect(Collectors.toList());
  }

  public void insertColumnPOs(TablePO tablePO, List<ColumnEntity> columnEntities) {
    List<ColumnPO> columnPOs =
        POConverters.initializeColumnPOs(tablePO, columnEntities, ColumnPO.ColumnOpType.CREATE);
    SessionUtils.doWithoutCommit(
        TableColumnMapper.class, mapper -> mapper.insertColumnPOs(columnPOs));
  }

  public boolean deleteColumnsByTableId(Long tableId) {
    Integer result =
        SessionUtils.doWithCommitAndFetchResult(
            TableColumnMapper.class, mapper -> mapper.softDeleteColumnsByTableId(tableId));
    return result > 0;
  }

  public int deleteColumnsByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithoutCommitAndFetchResult(
        TableColumnMapper.class,
        mapper -> mapper.deleteColumnPOsByLegacyTimeline(legacyTimeline, limit));
  }

  public boolean isColumnUpdated(TableEntity oldTable, TableEntity newTable) {
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

  public void updateColumnPOsFromTableDiff(
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
      if (oldColumn == null || !oldColumn.equals(newColumn)) {
        columnPOsToInsert.add(
            POConverters.initializeColumnPO(newTablePO, newColumn, ColumnPO.ColumnOpType.UPDATE));
      }
    }

    for (ColumnEntity oldColumn : oldColumns.values()) {
      if (!newColumns.containsKey(oldColumn.id())) {
        columnPOsToInsert.add(
            POConverters.initializeColumnPO(newTablePO, oldColumn, ColumnPO.ColumnOpType.DELETE));
      }
    }

    // If there is no change, directly return
    if (columnPOsToInsert.isEmpty()) {
      return;
    }

    SessionUtils.doWithoutCommit(
        TableColumnMapper.class, mapper -> mapper.insertColumnPOs(columnPOsToInsert));
  }
}
