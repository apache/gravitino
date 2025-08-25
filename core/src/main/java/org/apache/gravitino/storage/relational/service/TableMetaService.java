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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.po.ColumnPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/** The service class for table metadata. It provides the basic database operations for table. */
public class TableMetaService {
  private static final TableMetaService INSTANCE = new TableMetaService();

  public static TableMetaService getInstance() {
    return INSTANCE;
  }

  private TableMetaService() {}

  // Table may be deleted, so the TablePO may be null.
  public TablePO getTablePOById(Long tableId) {
    TablePO tablePO =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class, mapper -> mapper.selectTableMetaById(tableId));
    return tablePO;
  }

  public Long getTableIdBySchemaIdAndName(Long schemaId, String tableName) {
    Long tableId =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class,
            mapper -> mapper.selectTableIdBySchemaIdAndName(schemaId, tableName));

    if (tableId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TABLE.name().toLowerCase(),
          tableName);
    }
    return tableId;
  }

  public TableEntity getTableByIdentifier(NameIdentifier identifier) {
    NameIdentifierUtil.checkTable(identifier);

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

    TablePO tablePO = getTablePOBySchemaIdAndName(schemaId, identifier.name());
    List<ColumnPO> columnPOs =
        TableColumnMetaService.getInstance()
            .getColumnsByTableIdAndVersion(tablePO.getTableId(), tablePO.getCurrentVersion());

    return POConverters.fromTableAndColumnPOs(tablePO, columnPOs, identifier.namespace());
  }

  public List<TableEntity> listTablesByNamespace(Namespace namespace) {
    NamespaceUtil.checkTable(namespace);

    Long schemaId = CommonMetaService.getInstance().getParentEntityIdByNamespace(namespace);

    List<TablePO> tablePOs =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class, mapper -> mapper.listTablePOsBySchemaId(schemaId));

    return POConverters.fromTablePOs(tablePOs, namespace);
  }

  public void insertTable(TableEntity tableEntity, boolean overwrite) throws IOException {
    try {
      NameIdentifierUtil.checkTable(tableEntity.nameIdentifier());

      TablePO.Builder builder = TablePO.builder();
      fillTablePOBuilderParentEntityId(builder, tableEntity.namespace());

      AtomicReference<TablePO> tablePORef = new AtomicReference<>();
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  TableMetaMapper.class,
                  mapper -> {
                    TablePO po = POConverters.initializeTablePOWithVersion(tableEntity, builder);
                    tablePORef.set(po);
                    if (overwrite) {
                      mapper.insertTableMetaOnDuplicateKeyUpdate(po);
                    } else {
                      mapper.insertTableMeta(po);
                    }
                  }),
          () -> {
            // We need to delete the columns first if we want to overwrite the table.
            if (overwrite) {
              TableColumnMetaService.getInstance()
                  .deleteColumnsByTableId(tablePORef.get().getTableId());
            }
          },
          () -> {
            if (tableEntity.columns() != null && !tableEntity.columns().isEmpty()) {
              TableColumnMetaService.getInstance()
                  .insertColumnPOs(tablePORef.get(), tableEntity.columns());
            }
          });

    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.TABLE, tableEntity.nameIdentifier().toString());
      throw re;
    }
  }

  public <E extends Entity & HasIdentifier> TableEntity updateTable(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    NameIdentifierUtil.checkTable(identifier);

    String tableName = identifier.name();

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

    TablePO oldTablePO = getTablePOBySchemaIdAndName(schemaId, tableName);
    List<ColumnPO> oldTableColumns =
        TableColumnMetaService.getInstance()
            .getColumnsByTableIdAndVersion(oldTablePO.getTableId(), oldTablePO.getCurrentVersion());
    TableEntity oldTableEntity =
        POConverters.fromTableAndColumnPOs(oldTablePO, oldTableColumns, identifier.namespace());

    TableEntity newTableEntity = (TableEntity) updater.apply((E) oldTableEntity);
    Preconditions.checkArgument(
        Objects.equals(oldTableEntity.id(), newTableEntity.id()),
        "The updated table entity id: %s should be same with the table entity id before: %s",
        newTableEntity.id(),
        oldTableEntity.id());

    boolean isColumnChanged =
        TableColumnMetaService.getInstance().isColumnUpdated(oldTableEntity, newTableEntity);
    TablePO newTablePO =
        POConverters.updateTablePOWithVersion(oldTablePO, newTableEntity, isColumnChanged);

    final AtomicInteger updateResult = new AtomicInteger(0);
    try {
      SessionUtils.doMultipleWithCommit(
          () ->
              updateResult.set(
                  SessionUtils.getWithoutCommit(
                      TableMetaMapper.class,
                      mapper -> mapper.updateTableMeta(newTablePO, oldTablePO))),
          () -> {
            if (updateResult.get() > 0 && isColumnChanged) {
              TableColumnMetaService.getInstance()
                  .updateColumnPOsFromTableDiff(oldTableEntity, newTableEntity, newTablePO);
            }
          });

    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.TABLE, newTableEntity.nameIdentifier().toString());
      throw re;
    }

    if (updateResult.get() > 0) {
      return newTableEntity;
    } else {
      throw new IOException("Failed to update the entity: " + identifier);
    }
  }

  public boolean deleteTable(NameIdentifier identifier) {
    NameIdentifierUtil.checkTable(identifier);

    String tableName = identifier.name();

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

    Long tableId = getTableIdBySchemaIdAndName(schemaId, tableName);

    AtomicInteger deleteResult = new AtomicInteger(0);
    SessionUtils.doMultipleWithCommit(
        () ->
            deleteResult.set(
                SessionUtils.getWithoutCommit(
                    TableMetaMapper.class,
                    mapper -> mapper.softDeleteTableMetasByTableId(tableId))),
        () -> {
          if (deleteResult.get() > 0) {
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper ->
                    mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                        tableId, MetadataObject.Type.TABLE.name()));
            TableColumnMetaService.getInstance().deleteColumnsByTableId(tableId);
            SessionUtils.doWithoutCommit(
                SecurableObjectMapper.class,
                mapper ->
                    mapper.softDeleteObjectRelsByMetadataObject(
                        tableId, MetadataObject.Type.TABLE.name()));
            SessionUtils.doWithoutCommit(
                TagMetadataObjectRelMapper.class,
                mapper ->
                    mapper.softDeleteTagMetadataObjectRelsByMetadataObject(
                        tableId, MetadataObject.Type.TABLE.name()));
            SessionUtils.doWithoutCommit(
                TagMetadataObjectRelMapper.class,
                mapper -> mapper.softDeleteTagMetadataObjectRelsByTableId(tableId));

            SessionUtils.doWithoutCommit(
                StatisticMetaMapper.class,
                mapper -> mapper.softDeleteStatisticsByEntityId(tableId));
            SessionUtils.doWithoutCommit(
                PolicyMetadataObjectRelMapper.class,
                mapper -> mapper.softDeletePolicyMetadataObjectRelsByTableId(tableId));
          }
        });

    return deleteResult.get() > 0;
  }

  public int deleteTableMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        TableMetaMapper.class,
        mapper -> mapper.deleteTableMetasByLegacyTimeline(legacyTimeline, limit));
  }

  private void fillTablePOBuilderParentEntityId(TablePO.Builder builder, Namespace namespace) {
    NamespaceUtil.checkTable(namespace);
    Long[] parentEntityIds =
        CommonMetaService.getInstance().getParentEntityIdsByNamespace(namespace);
    builder.withMetalakeId(parentEntityIds[0]);
    builder.withCatalogId(parentEntityIds[1]);
    builder.withSchemaId(parentEntityIds[2]);
  }

  private TablePO getTablePOBySchemaIdAndName(Long schemaId, String tableName) {
    TablePO tablePO =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class,
            mapper -> mapper.selectTableMetaBySchemaIdAndName(schemaId, tableName));

    if (tablePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TABLE.name().toLowerCase(),
          tableName);
    }
    return tablePO;
  }
}
