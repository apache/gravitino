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
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetVersionMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

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

  // Schema may be deleted, so the SchemaPO may be null.
  public SchemaPO getSchemaPOById(Long schemaId) {
    return SessionUtils.getWithoutCommit(
        SchemaMetaMapper.class, mapper -> mapper.selectSchemaMetaById(schemaId));
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
    NameIdentifierUtil.checkSchema(identifier);
    String schemaName = identifier.name();

    Long catalogId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

    SchemaPO schemaPO = getSchemaPOByCatalogIdAndName(catalogId, schemaName);

    return POConverters.fromSchemaPO(schemaPO, identifier.namespace());
  }

  public List<SchemaEntity> listSchemasByNamespace(Namespace namespace) {
    NamespaceUtil.checkSchema(namespace);

    Long catalogId = CommonMetaService.getInstance().getParentEntityIdByNamespace(namespace);

    List<SchemaPO> schemaPOs =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class, mapper -> mapper.listSchemaPOsByCatalogId(catalogId));
    return POConverters.fromSchemaPOs(schemaPOs, namespace);
  }

  public void insertSchema(SchemaEntity schemaEntity, boolean overwrite) throws IOException {
    try {
      NameIdentifierUtil.checkSchema(schemaEntity.nameIdentifier());

      SchemaPO.Builder builder = SchemaPO.builder();
      fillSchemaPOBuilderParentEntityId(builder, schemaEntity.namespace());

      SessionUtils.doWithCommit(
          SchemaMetaMapper.class,
          mapper -> {
            SchemaPO po = POConverters.initializeSchemaPOWithVersion(schemaEntity, builder);
            if (overwrite) {
              mapper.insertSchemaMetaOnDuplicateKeyUpdate(po);
            } else {
              mapper.insertSchemaMeta(po);
            }
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.SCHEMA, schemaEntity.nameIdentifier().toString());
      throw re;
    }
  }

  public <E extends Entity & HasIdentifier> SchemaEntity updateSchema(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    NameIdentifierUtil.checkSchema(identifier);

    String schemaName = identifier.name();
    Long catalogId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

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
                      POConverters.updateSchemaPOWithVersion(oldSchemaPO, newEntity), oldSchemaPO));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
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
    NameIdentifierUtil.checkSchema(identifier);

    String schemaName = identifier.name();
    Long catalogId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());
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
                    TableColumnMapper.class,
                    mapper -> mapper.softDeleteColumnsBySchemaId(schemaId)),
            () ->
                SessionUtils.doWithoutCommit(
                    FilesetMetaMapper.class,
                    mapper -> mapper.softDeleteFilesetMetasBySchemaId(schemaId)),
            () ->
                SessionUtils.doWithoutCommit(
                    FilesetVersionMapper.class,
                    mapper -> mapper.softDeleteFilesetVersionsBySchemaId(schemaId)),
            () ->
                SessionUtils.doWithoutCommit(
                    TopicMetaMapper.class,
                    mapper -> mapper.softDeleteTopicMetasBySchemaId(schemaId)),
            () ->
                SessionUtils.doWithoutCommit(
                    OwnerMetaMapper.class,
                    mapper -> mapper.softDeleteOwnerRelBySchemaId(schemaId)));
      } else {
        List<TableEntity> tableEntities =
            TableMetaService.getInstance()
                .listTablesByNamespace(
                    NamespaceUtil.ofTable(
                        identifier.namespace().level(0),
                        identifier.namespace().level(1),
                        schemaName));
        if (!tableEntities.isEmpty()) {
          throw new NonEmptyEntityException(
              "Entity %s has sub-entities, you should remove sub-entities first", identifier);
        }
        List<FilesetEntity> filesetEntities =
            FilesetMetaService.getInstance()
                .listFilesetsByNamespace(
                    NamespaceUtil.ofFileset(
                        identifier.namespace().level(0),
                        identifier.namespace().level(1),
                        schemaName));
        if (!filesetEntities.isEmpty()) {
          throw new NonEmptyEntityException(
              "Entity %s has sub-entities, you should remove sub-entities first", identifier);
        }
        SessionUtils.doMultipleWithCommit(
            () ->
                SessionUtils.doWithoutCommit(
                    SchemaMetaMapper.class,
                    mapper -> mapper.softDeleteSchemaMetasBySchemaId(schemaId)),
            () ->
                SessionUtils.doWithoutCommit(
                    OwnerMetaMapper.class,
                    mapper ->
                        mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                            schemaId, MetadataObject.Type.SCHEMA.name())));
      }
    }
    return true;
  }

  public int deleteSchemaMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        SchemaMetaMapper.class,
        mapper -> {
          return mapper.deleteSchemaMetasByLegacyTimeline(legacyTimeline, limit);
        });
  }

  private void fillSchemaPOBuilderParentEntityId(SchemaPO.Builder builder, Namespace namespace) {
    NamespaceUtil.checkSchema(namespace);
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
          break;
      }
    }
  }
}
