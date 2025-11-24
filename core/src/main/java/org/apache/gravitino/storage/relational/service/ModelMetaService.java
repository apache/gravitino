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

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionAliasRelMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
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

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listModelsByNamespace")
  public List<ModelEntity> listModelsByNamespace(Namespace ns) {
    NamespaceUtil.checkModel(ns);

    Long schemaId =
        EntityIdService.getEntityId(NameIdentifier.of(ns.levels()), Entity.EntityType.SCHEMA);

    List<ModelPO> modelPOs =
        SessionUtils.getWithoutCommit(
            ModelMetaMapper.class, mapper -> mapper.listModelPOsBySchemaId(schemaId));

    return modelPOs.stream().map(m -> POConverters.fromModelPO(m, ns)).collect(Collectors.toList());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getModelByIdentifier")
  public ModelEntity getModelByIdentifier(NameIdentifier ident) {
    ModelPO modelPO = getModelPOByIdentifier(ident);
    return POConverters.fromModelPO(modelPO, ident.namespace());
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "insertModel")
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

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "deleteModel")
  public boolean deleteModel(NameIdentifier ident) {
    NameIdentifierUtil.checkModel(ident);

    Long schemaId;
    Long modelId;
    try {
      schemaId =
          EntityIdService.getEntityId(
              NameIdentifier.of(ident.namespace().levels()), Entity.EntityType.SCHEMA);
      modelId = getModelIdBySchemaIdAndModelName(schemaId, ident.name());
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
                SessionUtils.getWithoutCommit(
                    ModelMetaMapper.class,
                    mapper ->
                        mapper.softDeleteModelMetaBySchemaIdAndModelName(schemaId, ident.name()))),
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper ->
                    mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                        modelId, MetadataObject.Type.MODEL.name())),
        () ->
            SessionUtils.doWithoutCommit(
                SecurableObjectMapper.class,
                mapper ->
                    mapper.softDeleteObjectRelsByMetadataObject(
                        modelId, MetadataObject.Type.MODEL.name())),
        () ->
            SessionUtils.doWithoutCommit(
                TagMetadataObjectRelMapper.class,
                mapper ->
                    mapper.softDeleteTagMetadataObjectRelsByMetadataObject(
                        modelId, MetadataObject.Type.MODEL.name())),
        () ->
            SessionUtils.doWithoutCommit(
                StatisticMetaMapper.class,
                mapper -> mapper.softDeleteStatisticsByEntityId(modelId)),
        () ->
            SessionUtils.doWithoutCommit(
                PolicyMetadataObjectRelMapper.class,
                mapper ->
                    mapper.softDeletePolicyMetadataObjectRelsByMetadataObject(
                        modelId, MetadataObject.Type.MODEL.name())));

    return modelDeletedCount.get() > 0;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteModelMetasByLegacyTimeline")
  public int deleteModelMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        ModelMetaMapper.class,
        mapper -> mapper.deleteModelMetasByLegacyTimeline(legacyTimeline, limit));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getModelIdBySchemaIdAndModelName")
  public Long getModelIdBySchemaIdAndModelName(Long schemaId, String modelName) {
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

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getModelPOById")
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
    NamespacedEntityId namespacedEntityId =
        EntityIdService.getEntityIds(NameIdentifier.of(ns.levels()), Entity.EntityType.SCHEMA);
    builder.withMetalakeId(namespacedEntityId.namespaceIds()[0]);
    builder.withCatalogId(namespacedEntityId.namespaceIds()[1]);
    builder.withSchemaId(namespacedEntityId.entityId());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getModelPOByIdentifier")
  ModelPO getModelPOByIdentifier(NameIdentifier ident) {
    NameIdentifierUtil.checkModel(ident);

    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(ident.namespace().levels()), Entity.EntityType.SCHEMA);

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

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "updateModel")
  public <E extends Entity & HasIdentifier> ModelEntity updateModel(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    NameIdentifierUtil.checkModel(identifier);

    ModelPO oldModelPO = getModelPOByIdentifier(identifier);
    ModelEntity oldModelEntity = POConverters.fromModelPO(oldModelPO, identifier.namespace());
    ModelEntity newEntity = (ModelEntity) updater.apply((E) oldModelEntity);
    Preconditions.checkArgument(
        Objects.equals(oldModelEntity.id(), newEntity.id()),
        "The updated model entity id: %s should be same with the table entity id before: %s",
        newEntity.id(),
        oldModelEntity.id());

    Integer updateResult;
    try {
      updateResult =
          SessionUtils.doWithCommitAndFetchResult(
              ModelMetaMapper.class,
              mapper ->
                  mapper.updateModelMeta(
                      POConverters.updateModelPO(oldModelPO, newEntity), oldModelPO));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.MODEL, newEntity.nameIdentifier().toString());
      throw re;
    }

    if (updateResult > 0) {
      return newEntity;
    } else {
      throw new IOException("Failed to update the entity: " + identifier);
    }
  }
}
