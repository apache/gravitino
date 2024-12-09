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

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionAliasRelMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionMetaMapper;
import org.apache.gravitino.storage.relational.po.ModelPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelMetaService {

  private static final Logger LOG = LoggerFactory.getLogger(ModelMetaService.class);

  private static final ModelMetaService INSTANCE = new ModelMetaService();

  public static ModelMetaService getInstance() {
    return INSTANCE;
  }

  private ModelMetaService() {}

  public List<ModelEntity> listModelsByNamespace(Namespace ns) {
    NamespaceUtil.checkModel(ns);

    Long schemaId = CommonMetaService.getInstance().getParentEntityIdByNamespace(ns);

    List<ModelPO> modelPOs =
        SessionUtils.getWithoutCommit(
            ModelMetaMapper.class, mapper -> mapper.listModelPOsBySchemaId(schemaId));

    return modelPOs.stream().map(m -> POConverters.fromModelPO(m, ns)).collect(Collectors.toList());
  }

  public ModelEntity getModelByIdentifier(NameIdentifier ident) {
    ModelPO modelPO = getModelPOByIdentifier(ident);
    return POConverters.fromModelPO(modelPO, ident.namespace());
  }

  public void insertModel(ModelEntity modelEntity, boolean overwrite) throws IOException {
    NameIdentifierUtil.checkModel(modelEntity.nameIdentifier());

    try {
      ModelPO.Builder builder = ModelPO.builder();
      fillModelPOBuilderParentEntityId(builder, modelEntity.namespace());

      SessionUtils.doWithCommit(
          ModelMetaMapper.class,
          mapper -> {
            ModelPO po = POConverters.initializeModelPO(modelEntity, builder);
            if (overwrite) {
              mapper.insertModelMetaOnDuplicateKeyUpdate(po);
            } else {
              mapper.insertModelMeta(po);
            }
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.MODEL, modelEntity.nameIdentifier().toString());
      throw re;
    }
  }

  public boolean deleteModel(NameIdentifier ident) {
    NameIdentifierUtil.checkModel(ident);

    Long schemaId;
    try {
      schemaId = CommonMetaService.getInstance().getParentEntityIdByNamespace(ident.namespace());
    } catch (NoSuchEntityException e) {
      LOG.warn("Failed to delete model: {}", ident, e);
      return false;
    }

    AtomicInteger modelDeletedCount = new AtomicInteger();
    SessionUtils.doMultipleWithCommit(
        // delete model versions first
        () ->
            SessionUtils.doWithoutCommit(
                ModelVersionMetaMapper.class,
                mapper ->
                    mapper.softDeleteModelVersionsBySchemaIdAndModelName(schemaId, ident.name())),

        // delete model version aliases
        () ->
            SessionUtils.doWithoutCommit(
                ModelVersionAliasRelMapper.class,
                mapper ->
                    mapper.softDeleteModelVersionAliasRelsBySchemaIdAndModelName(
                        schemaId, ident.name())),

        // delete model meta
        () ->
            modelDeletedCount.set(
                SessionUtils.doWithoutCommitAndFetchResult(
                    ModelMetaMapper.class,
                    mapper ->
                        mapper.softDeleteModelMetaBySchemaIdAndModelName(schemaId, ident.name()))));

    return modelDeletedCount.get() > 0;
  }

  public int deleteModelMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        ModelMetaMapper.class,
        mapper -> mapper.deleteModelMetasByLegacyTimeline(legacyTimeline, limit));
  }

  Long getModelIdBySchemaIdAndModelName(Long schemaId, String modelName) {
    Long modelId =
        SessionUtils.getWithoutCommit(
            ModelMetaMapper.class,
            mapper -> mapper.selectModelIdBySchemaIdAndModelName(schemaId, modelName));

    if (modelId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.MODEL.name().toLowerCase(Locale.ROOT),
          modelName);
    }

    return modelId;
  }

  ModelPO getModelPOById(Long modelId) {
    ModelPO modelPO =
        SessionUtils.getWithoutCommit(
            ModelMetaMapper.class, mapper -> mapper.selectModelMetaByModelId(modelId));

    if (modelPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.MODEL.name().toLowerCase(Locale.ROOT),
          modelId.toString());
    }

    return modelPO;
  }

  private void fillModelPOBuilderParentEntityId(ModelPO.Builder builder, Namespace ns) {
    NamespaceUtil.checkModel(ns);
    String metalake = ns.level(0);
    String catalog = ns.level(1);
    String schema = ns.level(2);

    Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
    builder.withMetalakeId(metalakeId);

    Long catalogId =
        CatalogMetaService.getInstance().getCatalogIdByMetalakeIdAndName(metalakeId, catalog);
    builder.withCatalogId(catalogId);

    Long schemaId =
        SchemaMetaService.getInstance().getSchemaIdByCatalogIdAndName(catalogId, schema);
    builder.withSchemaId(schemaId);
  }

  ModelPO getModelPOByIdentifier(NameIdentifier ident) {
    NameIdentifierUtil.checkModel(ident);

    Long schemaId = CommonMetaService.getInstance().getParentEntityIdByNamespace(ident.namespace());

    ModelPO modelPO =
        SessionUtils.getWithoutCommit(
            ModelMetaMapper.class,
            mapper -> mapper.selectModelMetaBySchemaIdAndModelName(schemaId, ident.name()));

    if (modelPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.MODEL.name().toLowerCase(Locale.ROOT),
          ident.toString());
    }
    return modelPO;
  }
}
