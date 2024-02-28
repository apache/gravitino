/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.storage.relational.mapper.CatalogMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.SchemaMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.TableMetaMapper;
import com.datastrato.gravitino.storage.relational.po.TablePO;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.sql.SQLIntegrityConstraintViolationException;
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

  public TableEntity getTableByIdentifier(NameIdentifier identifier) {
    NameIdentifier.checkTable(identifier);
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String schemaName = identifier.namespace().level(2);
    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
    if (metalakeId == null) {
      throw new NoSuchEntityException(NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, metalakeName);
    }

    Long catalogId =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogIdByMetalakeIdAndName(metalakeId, catalogName));
    if (catalogId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          String.format("%s.%s", metalakeName, catalogName));
    }

    Long schemaId =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.selectSchemaIdByCatalogIdAndName(catalogId, schemaName));
    if (schemaId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, identifier.namespace().toString());
    }

    TablePO tablePO =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class,
            mapper -> mapper.selectTableMetaBySchemaIdAndName(schemaId, identifier.name()));

    if (tablePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, identifier.toString());
    }

    return POConverters.fromTablePO(tablePO, identifier.namespace());
  }

  public List<TableEntity> listTablesByNamespace(Namespace namespace) {
    Namespace.checkTable(namespace);
    String metalakeName = namespace.level(0);
    String catalogName = namespace.level(1);
    String schemaName = namespace.level(2);

    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
    if (metalakeId == null) {
      throw new NoSuchEntityException(NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, metalakeName);
    }

    Long catalogId =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogIdByMetalakeIdAndName(metalakeId, catalogName));
    if (catalogId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          String.format("%s.%s", metalakeName, catalogName));
    }

    Long schemaId =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.selectSchemaIdByCatalogIdAndName(catalogId, schemaName));
    if (schemaId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, namespace.toString());
    }

    List<TablePO> tablePOs =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class, mapper -> mapper.listTablePOsBySchemaId(schemaId));
    return POConverters.fromTablePOs(tablePOs, namespace);
  }

  public void insertTable(TableEntity tableEntity, boolean overwrite) {
    try {
      NameIdentifier.checkTable(tableEntity.nameIdentifier());
      String metalakeName = tableEntity.namespace().level(0);
      String catalogName = tableEntity.namespace().level(1);
      String schemaName = tableEntity.namespace().level(2);
      Long metalakeId =
          SessionUtils.getWithoutCommit(
              MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
      if (metalakeId == null) {
        throw new NoSuchEntityException(NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, metalakeName);
      }

      Long catalogId =
          SessionUtils.getWithoutCommit(
              CatalogMetaMapper.class,
              mapper -> mapper.selectCatalogIdByMetalakeIdAndName(metalakeId, catalogName));
      if (catalogId == null) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
            String.format("%s.%s", metalakeName, catalogName));
      }

      Long schemaId =
          SessionUtils.getWithoutCommit(
              SchemaMetaMapper.class,
              mapper -> mapper.selectSchemaIdByCatalogIdAndName(catalogId, schemaName));
      if (schemaId == null) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, tableEntity.namespace().toString());
      }

      SessionUtils.doWithCommit(
          TableMetaMapper.class,
          mapper -> {
            TablePO po =
                POConverters.initializeTablePOWithVersion(
                    tableEntity, metalakeId, catalogId, schemaId);
            if (overwrite) {
              mapper.insertTableMetaOnDuplicateKeyUpdate(po);
            } else {
              mapper.insertTableMeta(po);
            }
          });
    } catch (RuntimeException re) {
      if (re.getCause() != null
          && re.getCause().getCause() != null
          && re.getCause().getCause() instanceof SQLIntegrityConstraintViolationException) {
        // TODO We should make more fine-grained exception judgments
        // Usually throwing `SQLIntegrityConstraintViolationException` means that
        // SQL violates the constraints of `primary key` and `unique key`.
        // We simply think that the entity already exists at this time.
        throw new EntityAlreadyExistsException(
            String.format("Table entity: %s already exists", tableEntity.nameIdentifier()));
      }
      throw re;
    }
  }

  public <E extends Entity & HasIdentifier> TableEntity updateTable(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    NameIdentifier.checkTable(identifier);
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String schemaName = identifier.namespace().level(2);
    String tableName = identifier.name();

    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
    if (metalakeId == null) {
      throw new NoSuchEntityException(NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, metalakeName);
    }

    Long catalogId =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogIdByMetalakeIdAndName(metalakeId, catalogName));
    if (catalogId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          String.format("%s.%s", metalakeName, catalogName));
    }

    Long schemaId =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.selectSchemaIdByCatalogIdAndName(catalogId, schemaName));
    if (schemaId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, identifier.namespace().toString());
    }

    TablePO oldTablePO =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class,
            mapper -> mapper.selectTableMetaBySchemaIdAndName(schemaId, tableName));
    if (oldTablePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, identifier.toString());
    }

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
                      POConverters.updateTablePOWithVersion(
                          oldTablePO, newEntity, metalakeId, catalogId, schemaId),
                      oldTablePO));
    } catch (RuntimeException re) {
      if (re.getCause() != null
          && re.getCause().getCause() != null
          && re.getCause().getCause() instanceof SQLIntegrityConstraintViolationException) {
        // TODO We should make more fine-grained exception judgments
        // Usually throwing `SQLIntegrityConstraintViolationException` means that
        // SQL violates the constraints of `primary key` and `unique key`.
        // We simply think that the entity already exists at this time.
        throw new EntityAlreadyExistsException(
            String.format("Table entity: %s already exists", newEntity.nameIdentifier()));
      }
      throw re;
    }

    if (updateResult > 0) {
      return newEntity;
    } else {
      throw new IOException("Failed to update the entity: " + identifier);
    }
  }

  public boolean deleteTable(NameIdentifier identifier, boolean cascade) {
    NameIdentifier.checkTable(identifier);
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String schemaName = identifier.namespace().level(2);
    String tableName = identifier.name();
    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
    if (metalakeId == null) {
      throw new NoSuchEntityException(NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, metalakeName);
    }

    Long catalogId =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogIdByMetalakeIdAndName(metalakeId, catalogName));
    if (catalogId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          String.format("%s.%s", metalakeName, catalogName));
    }

    Long schemaId =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.selectSchemaIdByCatalogIdAndName(catalogId, schemaName));
    if (schemaId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, identifier.namespace().toString());
    }

    Long tableId =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class,
            mapper -> mapper.selectTableIdBySchemaIdAndName(schemaId, tableName));
    if (tableId != null) {
      if (cascade) {
        SessionUtils.doMultipleWithCommit(
            () ->
                SessionUtils.doWithoutCommit(
                    TableMetaMapper.class, mapper -> mapper.softDeleteTableMetasByTableId(tableId)),
            () -> {
              // TODO We will cascade delete the metadata of sub-resources under the catalog
            });
      } else {
        // TODO Check whether the sub-resources are empty. If the sub-resources are not empty,
        //  deletion is not allowed.
        SessionUtils.doWithCommit(
            TableMetaMapper.class, mapper -> mapper.softDeleteTableMetasByTableId(tableId));
      }
    }
    return true;
  }
}
