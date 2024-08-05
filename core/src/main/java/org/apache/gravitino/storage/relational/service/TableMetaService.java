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
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
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

  public TablePO getTablePOBySchemaIdAndName(Long schemaId, String tableName) {
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

    return POConverters.fromTablePO(tablePO, identifier.namespace());
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

      SessionUtils.doWithCommit(
          TableMetaMapper.class,
          mapper -> {
            TablePO po = POConverters.initializeTablePOWithVersion(tableEntity, builder);
            if (overwrite) {
              mapper.insertTableMetaOnDuplicateKeyUpdate(po);
            } else {
              mapper.insertTableMeta(po);
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
    TableEntity oldTableEntity = POConverters.fromTablePO(oldTablePO, identifier.namespace());
    TableEntity newEntity = (TableEntity) updater.apply((E) oldTableEntity);
    Preconditions.checkArgument(
        Objects.equals(oldTableEntity.id(), newEntity.id()),
        "The updated table entity id: %s should be same with the table entity id before: %s",
        newEntity.id(),
        oldTableEntity.id());

    Integer updateResult;
    try {
      updateResult =
          SessionUtils.doWithCommitAndFetchResult(
              TableMetaMapper.class,
              mapper ->
                  mapper.updateTableMeta(
                      POConverters.updateTablePOWithVersion(oldTablePO, newEntity), oldTablePO));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.TABLE, newEntity.nameIdentifier().toString());
      throw re;
    }

    if (updateResult > 0) {
      return newEntity;
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

    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                TableMetaMapper.class, mapper -> mapper.softDeleteTableMetasByTableId(tableId)),
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper ->
                    mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                        tableId, MetadataObject.Type.TABLE.name())));

    return true;
  }

  public int deleteTableMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        TableMetaMapper.class,
        mapper -> {
          return mapper.deleteTableMetasByLegacyTimeline(legacyTimeline, limit);
        });
  }

  private void fillTablePOBuilderParentEntityId(TablePO.Builder builder, Namespace namespace) {
    NamespaceUtil.checkTable(namespace);
    Long parentEntityId = null;
    for (int level = 0; level < namespace.levels().length; level++) {
      String name = namespace.level(level);
      switch (level) {
        case 0:
          parentEntityId = MetalakeMetaService.getInstance().getMetalakeIdByName(name);
          builder.withMetalakeId(parentEntityId);
          continue;
        case 1:
          parentEntityId =
              CatalogMetaService.getInstance()
                  .getCatalogIdByMetalakeIdAndName(parentEntityId, name);
          builder.withCatalogId(parentEntityId);
          continue;
        case 2:
          parentEntityId =
              SchemaMetaService.getInstance().getSchemaIdByCatalogIdAndName(parentEntityId, name);
          builder.withSchemaId(parentEntityId);
          break;
      }
    }
  }
}
