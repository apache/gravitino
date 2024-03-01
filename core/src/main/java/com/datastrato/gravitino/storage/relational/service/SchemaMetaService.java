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
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.storage.relational.mapper.SchemaMetaMapper;
import com.datastrato.gravitino.storage.relational.po.SchemaPO;
import com.datastrato.gravitino.storage.relational.utils.ExceptionUtils;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
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
    String schemaName = identifier.name();

    Long catalogId = CommonMetaService.getInstance().getParentIdByNamespace(identifier.namespace());

    SchemaPO schemaPO = getSchemaPOByCatalogIdAndName(catalogId, schemaName);

    return POConverters.fromSchemaPO(schemaPO, identifier.namespace());
  }

  public List<SchemaEntity> listSchemasByNamespace(Namespace namespace) {
    Namespace.checkSchema(namespace);

    Long catalogId = CommonMetaService.getInstance().getParentIdByNamespace(namespace);

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
      ExceptionUtils.checkSQLConstraintException(
          re, Entity.EntityType.SCHEMA, schemaEntity.nameIdentifier().toString());
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
      ExceptionUtils.checkSQLConstraintException(
          re, Entity.EntityType.SCHEMA, newEntity.nameIdentifier().toString());
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

    String schemaName = identifier.name();
    Long catalogId = CommonMetaService.getInstance().getParentIdByNamespace(identifier.namespace());
    Long schemaId = getSchemaIdByCatalogIdAndName(catalogId, schemaName);

    if (schemaId != null) {
      if (cascade) {
        SessionUtils.doMultipleWithCommit(
            () ->
                SessionUtils.doWithoutCommit(
                    SchemaMetaMapper.class,
                    mapper -> mapper.softDeleteSchemaMetasBySchemaId(schemaId)),
            () -> {
              // TODO We will cascade delete the metadata of sub-resources under the schema
            });
      } else {
        // TODO Check whether the sub-resources are empty. If the sub-resources are not empty,
        //  deletion is not allowed.
        SessionUtils.doWithCommit(
            SchemaMetaMapper.class, mapper -> mapper.softDeleteSchemaMetasBySchemaId(schemaId));
      }
    }
    return true;
  }
}
