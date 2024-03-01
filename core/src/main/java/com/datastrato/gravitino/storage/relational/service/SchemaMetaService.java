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
import com.datastrato.gravitino.exceptions.NonEmptyEntityException;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.storage.relational.mapper.FilesetMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.FilesetVersionMapper;
import com.datastrato.gravitino.storage.relational.mapper.SchemaMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.TableMetaMapper;
import com.datastrato.gravitino.storage.relational.po.SchemaPO;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/** The service class for schema metadata. It provides the basic database operations for schema. */
public class SchemaMetaService {
  private static final SchemaMetaService INSTANCE = new SchemaMetaService();

  public static SchemaMetaService getInstance() {
    return INSTANCE;
  }

  private SchemaMetaService() {}

  public SchemaPO getSchemaPOByCatalogIdAndName(Long catalogId, String schemaName) {
    SchemaPO schemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.selectSchemaMetaByCatalogIdAndName(catalogId, schemaName));

    if (schemaPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          schemaName);
    }
    return schemaPO;
  }

  public Long getSchemaIdByCatalogIdAndName(Long catalogId, String schemaName) {
    Long schemaId =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.selectSchemaIdByCatalogIdAndName(catalogId, schemaName));

    if (schemaId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          schemaName);
    }
    return schemaId;
  }

  public SchemaEntity getSchemaByIdentifier(NameIdentifier identifier) {
    NameIdentifier.checkSchema(identifier);
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String schemaName = identifier.name();

    Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);

    Long catalogId =
        CatalogMetaService.getInstance().getCatalogIdByMetalakeIdAndName(metalakeId, catalogName);

    SchemaPO schemaPO = getSchemaPOByCatalogIdAndName(catalogId, schemaName);

    return POConverters.fromSchemaPO(schemaPO, identifier.namespace());
  }

  public List<SchemaEntity> listSchemasByNamespace(Namespace namespace) {
    Namespace.checkSchema(namespace);
    String metalakeName = namespace.level(0);
    String catalogName = namespace.level(1);

    Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);
    Long catalogId =
        CatalogMetaService.getInstance().getCatalogIdByMetalakeIdAndName(metalakeId, catalogName);

    List<SchemaPO> schemaPOs =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class, mapper -> mapper.listSchemaPOsByCatalogId(catalogId));
    return POConverters.fromSchemaPOs(schemaPOs, namespace);
  }

  public void insertSchema(SchemaEntity schemaEntity, boolean overwrite) {
    try {
      NameIdentifier.checkSchema(schemaEntity.nameIdentifier());
      String metalakeName = schemaEntity.namespace().level(0);
      String catalogName = schemaEntity.namespace().level(1);

      Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);
      Long catalogId =
          CatalogMetaService.getInstance().getCatalogIdByMetalakeIdAndName(metalakeId, catalogName);

      SessionUtils.doWithCommit(
          SchemaMetaMapper.class,
          mapper -> {
            SchemaPO po =
                POConverters.initializeSchemaPOWithVersion(schemaEntity, metalakeId, catalogId);
            if (overwrite) {
              mapper.insertSchemaMetaOnDuplicateKeyUpdate(po);
            } else {
              mapper.insertSchemaMeta(po);
            }
          });
    } catch (RuntimeException re) {
      if (re.getCause() != null
          && re.getCause() instanceof SQLIntegrityConstraintViolationException) {
        // TODO We should make more fine-grained exception judgments
        // Usually throwing `SQLIntegrityConstraintViolationException` means that
        // SQL violates the constraints of `primary key` and `unique key`.
        // We simply think that the entity already exists at this time.
        throw new EntityAlreadyExistsException(
            String.format("Schema entity: %s already exists", schemaEntity.nameIdentifier()));
      }
      throw re;
    }
  }

  public <E extends Entity & HasIdentifier> SchemaEntity updateSchema(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    NameIdentifier.checkSchema(identifier);
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String schemaName = identifier.name();

    Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);

    Long catalogId =
        CatalogMetaService.getInstance().getCatalogIdByMetalakeIdAndName(metalakeId, catalogName);

    SchemaPO oldSchemaPO = getSchemaPOByCatalogIdAndName(catalogId, schemaName);

    SchemaEntity oldSchemaEntity = POConverters.fromSchemaPO(oldSchemaPO, identifier.namespace());
    SchemaEntity newEntity = (SchemaEntity) updater.apply((E) oldSchemaEntity);
    Preconditions.checkArgument(
        Objects.equals(oldSchemaEntity.id(), newEntity.id()),
        "The updated schema entity id: %s should be same with the schema entity id before: %s",
        newEntity.id(),
        oldSchemaEntity.id());

    Integer updateResult;
    try {
      updateResult =
          SessionUtils.doWithCommitAndFetchResult(
              SchemaMetaMapper.class,
              mapper ->
                  mapper.updateSchemaMeta(
                      POConverters.updateSchemaPOWithVersion(
                          oldSchemaPO, newEntity, metalakeId, catalogId),
                      oldSchemaPO));
    } catch (RuntimeException re) {
      if (re.getCause() != null
          && re.getCause() instanceof SQLIntegrityConstraintViolationException) {
        // TODO We should make more fine-grained exception judgments
        // Usually throwing `SQLIntegrityConstraintViolationException` means that
        // SQL violates the constraints of `primary key` and `unique key`.
        // We simply think that the entity already exists at this time.
        throw new EntityAlreadyExistsException(
            String.format("Schema entity: %s already exists", newEntity.nameIdentifier()));
      }
      throw re;
    }

    if (updateResult > 0) {
      return newEntity;
    } else {
      throw new IOException("Failed to update the entity: " + identifier);
    }
  }

  public boolean deleteSchema(NameIdentifier identifier, boolean cascade) {
    NameIdentifier.checkSchema(identifier);
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String schemaName = identifier.name();
    Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);

    Long catalogId =
        CatalogMetaService.getInstance().getCatalogIdByMetalakeIdAndName(metalakeId, catalogName);

    Long schemaId = getSchemaIdByCatalogIdAndName(catalogId, schemaName);

    if (schemaId != null) {
      if (cascade) {
        SessionUtils.doMultipleWithCommit(
            () ->
                SessionUtils.doWithoutCommit(
                    SchemaMetaMapper.class,
                    mapper -> mapper.softDeleteSchemaMetasBySchemaId(schemaId)),
            () ->
                SessionUtils.doWithoutCommit(
                    TableMetaMapper.class,
                    mapper -> mapper.softDeleteTableMetasBySchemaId(schemaId)),
            () ->
                SessionUtils.doWithoutCommit(
                    FilesetMetaMapper.class,
                    mapper -> mapper.softDeleteFilesetMetasBySchemaId(schemaId)),
            () ->
                SessionUtils.doWithoutCommit(
                    FilesetVersionMapper.class,
                    mapper -> mapper.softDeleteFilesetVersionsBySchemaId(schemaId)));
      } else {
        List<TableEntity> tableEntities =
            TableMetaService.getInstance()
                .listTablesByNamespace(Namespace.ofTable(metalakeName, catalogName, schemaName));
        if (!tableEntities.isEmpty()) {
          throw new NonEmptyEntityException(
              "Entity %s has sub-entities, you should remove sub-entities first", identifier);
        }
        List<FilesetEntity> filesetEntities =
            FilesetMetaService.getInstance()
                .listFilesetsByNamespace(
                    Namespace.ofFileset(metalakeName, catalogName, schemaName));
        if (!filesetEntities.isEmpty()) {
          throw new NonEmptyEntityException(
              "Entity %s has sub-entities, you should remove sub-entities first", identifier);
        }
        SessionUtils.doWithCommit(
            SchemaMetaMapper.class, mapper -> mapper.softDeleteSchemaMetasBySchemaId(schemaId));
      }
    }
    return true;
  }
}
