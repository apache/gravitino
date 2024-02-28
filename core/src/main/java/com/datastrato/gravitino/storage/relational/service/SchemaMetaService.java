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
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.storage.relational.mapper.CatalogMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.SchemaMetaMapper;
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

  public SchemaEntity getSchemaByIdentifier(NameIdentifier identifier) {
    NameIdentifier.checkSchema(identifier);
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
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
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, identifier.namespace().toString());
    }

    SchemaPO schemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.selectSchemaMetaByMetalakeIdAndCatalogIdAndName(
                    metalakeId, catalogId, identifier.name()));

    if (schemaPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, identifier.toString());
    }

    return POConverters.fromSchemaPO(schemaPO, identifier.namespace());
  }

  public List<SchemaEntity> listSchemasByNamespace(Namespace namespace) {
    Namespace.checkSchema(namespace);
    String metalakeName = namespace.level(0);
    String catalogName = namespace.level(1);
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
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, namespace.toString());
    }
    List<SchemaPO> schemaPOs =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.listSchemaPOsByMetalakeIdAndCatalogId(metalakeId, catalogId));
    return POConverters.fromSchemaPOs(schemaPOs, namespace);
  }

  public void insertSchema(SchemaEntity schemaEntity, boolean overwrite) {
    try {
      NameIdentifier.checkSchema(schemaEntity.nameIdentifier());
      Long metalakeId =
          SessionUtils.getWithoutCommit(
              MetalakeMetaMapper.class,
              mapper -> mapper.selectMetalakeIdMetaByName(schemaEntity.namespace().level(0)));
      if (metalakeId == null) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, schemaEntity.namespace().level(0));
      }
      Long catalogId =
          SessionUtils.getWithoutCommit(
              CatalogMetaMapper.class,
              mapper ->
                  mapper.selectCatalogIdByMetalakeIdAndName(
                      metalakeId, schemaEntity.namespace().level(1)));
      if (catalogId == null) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, schemaEntity.namespace());
      }

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
          && re.getCause().getCause() != null
          && re.getCause().getCause() instanceof SQLIntegrityConstraintViolationException) {
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
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, identifier.namespace().toString());
    }

    SchemaPO oldSchemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.selectSchemaMetaByMetalakeIdAndCatalogIdAndName(
                    metalakeId, catalogId, schemaName));
    if (oldSchemaPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, identifier.toString());
    }

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
          && re.getCause().getCause() != null
          && re.getCause().getCause() instanceof SQLIntegrityConstraintViolationException) {
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
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, identifier.namespace().toString());
    }

    Long schemaId =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.selectSchemaIdByMetalakeIdAndCatalogIdAndName(
                    metalakeId, catalogId, schemaName));
    if (schemaId != null) {
      if (cascade) {
        SessionUtils.doMultipleWithCommit(
            () ->
                SessionUtils.doWithoutCommit(
                    SchemaMetaMapper.class,
                    mapper -> mapper.softDeleteSchemaMetasBySchemaId(schemaId)),
            () -> {
              // TODO We will cascade delete the metadata of sub-resources under the catalog
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
