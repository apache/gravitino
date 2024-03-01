/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.storage.relational.mapper.TableMetaMapper;
import com.datastrato.gravitino.storage.relational.po.TablePO;
import com.datastrato.gravitino.storage.relational.utils.ExceptionUtils;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

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
    NameIdentifier.checkTable(identifier);

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

    TablePO tablePO = getTablePOBySchemaIdAndName(schemaId, identifier.name());

    return POConverters.fromTablePO(tablePO, identifier.namespace());
  }

  public List<TableEntity> listTablesByNamespace(Namespace namespace) {
    Namespace.checkTable(namespace);

    Long schemaId = CommonMetaService.getInstance().getParentEntityIdByNamespace(namespace);

    List<TablePO> tablePOs =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class, mapper -> mapper.listTablePOsBySchemaId(schemaId));

    return POConverters.fromTablePOs(tablePOs, namespace);
  }

  public void insertTable(TableEntity tableEntity, boolean overwrite) {
    try {
      NameIdentifier.checkTable(tableEntity.nameIdentifier());

      TablePO.Builder builder = new TablePO.Builder();
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
      ExceptionUtils.checkSQLConstraintException(
          re, Entity.EntityType.TABLE, tableEntity.nameIdentifier().toString());
      throw re;
    }
  }

  public <E extends Entity & HasIdentifier> TableEntity updateTable(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    NameIdentifier.checkTable(identifier);

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
      ExceptionUtils.checkSQLConstraintException(
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
    NameIdentifier.checkTable(identifier);

    String tableName = identifier.name();

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

    Long tableId = getTableIdBySchemaIdAndName(schemaId, tableName);

    SessionUtils.doWithCommit(
        TableMetaMapper.class, mapper -> mapper.softDeleteTableMetasByTableId(tableId));

    return true;
  }

  private void fillTablePOBuilderParentEntityId(TablePO.Builder builder, Namespace namespace) {
    Namespace.checkTable(namespace);
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
