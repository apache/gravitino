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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.EntityChangeLogNameIdentifierCodec;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionAliasRelMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.po.ModelPO;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
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

    List<ModelPO> modelPOs = listModelPOs(ns);
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
      ModelPO po = POConverters.initializeModelPO(modelEntity, builder);

      SessionUtils.doMultipleWithCommit(
          // Hold the parent schema row until this transaction ends, so the model cannot be
          // written below a schema that is being dropped.
          () ->
              SchemaMetaService.getInstance()
                  .lockSchemaForEntityWrite(
                      modelEntity.nameIdentifier(),
                      po.getSchemaId(),
                      po.getCatalogId(),
                      po.getMetalakeId()),
          () ->
              SessionUtils.doWithoutCommit(
                  ModelMetaMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertModelMetaOnDuplicateKeyUpdate(po);
                    } else {
                      mapper.insertModelMeta(po);
                    }
                  }));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.MODEL, modelEntity.nameIdentifier().toString());
      throw re;
    }
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "deleteModel")
  public boolean deleteModel(NameIdentifier ident) {
    ModelPO modelPO;
    try {
      modelPO = getModelPOByIdentifier(ident);
    } catch (NoSuchEntityException e) {
      LOG.warn("Failed to delete model: {}", ident, e);
      return false;
    }
    String metalakeName = ident.namespace().level(0);
    String catalogName = ident.namespace().level(1);
    String schemaName = ident.namespace().level(2);
    String modelFullName =
        EntityChangeLogNameIdentifierCodec.encode(
            NameIdentifierUtil.ofModel(metalakeName, catalogName, schemaName, ident.name()));

    // Delete the model row first, and only when its concurrency version still matches the value
    // read above. If another writer changed the model, stop before removing any related data.
    try {
      SessionUtils.doMultipleWithCommit(
          () -> deleteModelWithVersion(ident, modelPO),
          () -> deleteModelDependents(modelPO),
          () -> {
            SessionUtils.doWithoutCommit(
                EntityChangeLogMapper.class,
                mapper ->
                    mapper.insertEntityChange(
                        metalakeName,
                        Entity.EntityType.MODEL.name(),
                        modelFullName,
                        OperateType.DROP));
          });
    } catch (NoSuchEntityException e) {
      // Another writer dropped the model between the read above and this transaction. A drop that
      // finds nothing to drop is reported the same way as the read above reports it, so that a
      // duplicate drop stays a plain "false" instead of surfacing as an error.
      LOG.warn("Failed to delete model: {}", ident, e);
      return false;
    }

    return true;
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

    return modelPOFetcher().apply(ident);
  }

  private List<ModelPO> listModelPOs(Namespace namespace) {
    return modelListFetcher().apply(namespace);
  }

  private List<ModelPO> listModelPOsBySchemaId(Namespace namespace) {
    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(namespace.levels()), Entity.EntityType.SCHEMA);
    return SessionUtils.getWithoutCommit(
        ModelMetaMapper.class, mapper -> mapper.listModelPOsBySchemaId(schemaId));
  }

  private List<ModelPO> listModelPOsByFullQualifiedName(Namespace namespace) {
    String[] namespaceLevels = namespace.levels();
    List<ModelPO> modelPOs =
        SessionUtils.getWithoutCommit(
            ModelMetaMapper.class,
            mapper ->
                mapper.listModelPOsByFullQualifiedName(
                    namespaceLevels[0], namespaceLevels[1], namespaceLevels[2]));
    if (modelPOs.isEmpty() || modelPOs.get(0).getSchemaId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(Locale.ROOT),
          namespaceLevels[2]);
    }
    return modelPOs.stream().filter(po -> po.getModelId() != null).collect(Collectors.toList());
  }

  private ModelPO getModelPOBySchemaId(NameIdentifier identifier) {
    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(identifier.namespace().levels()), Entity.EntityType.SCHEMA);

    ModelPO modelPO =
        SessionUtils.getWithoutCommit(
            ModelMetaMapper.class,
            mapper -> mapper.selectModelMetaBySchemaIdAndModelName(schemaId, identifier.name()));

    if (modelPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.MODEL.name().toLowerCase(Locale.ROOT),
          identifier.toString());
    }
    return modelPO;
  }

  private ModelPO getModelPOByFullQualifiedName(NameIdentifier identifier) {
    String[] namespaceLevels = identifier.namespace().levels();
    ModelPO modelPO =
        SessionUtils.getWithoutCommit(
            ModelMetaMapper.class,
            mapper ->
                mapper.selectModelByFullQualifiedName(
                    namespaceLevels[0], namespaceLevels[1], namespaceLevels[2], identifier.name()));

    if (modelPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.MODEL.name().toLowerCase(Locale.ROOT),
          identifier.toString());
    }

    if (modelPO.getSchemaId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(Locale.ROOT),
          namespaceLevels[2]);
    }

    if (modelPO.getModelId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.MODEL.name().toLowerCase(Locale.ROOT),
          identifier.toString());
    }
    return modelPO;
  }

  private Function<Namespace, List<ModelPO>> modelListFetcher() {
    return GravitinoEnv.getInstance().cacheEnabled()
        ? this::listModelPOsBySchemaId
        : this::listModelPOsByFullQualifiedName;
  }

  private Function<NameIdentifier, ModelPO> modelPOFetcher() {
    return GravitinoEnv.getInstance().cacheEnabled()
        ? this::getModelPOBySchemaId
        : this::getModelPOByFullQualifiedName;
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

    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String schemaName = identifier.namespace().level(2);
    String oldFullName =
        EntityChangeLogNameIdentifierCodec.encode(
            NameIdentifierUtil.ofModel(
                metalakeName, catalogName, schemaName, oldModelEntity.name()));
    boolean isRenamed = !Objects.equals(oldModelEntity.name(), newEntity.name());

    try {
      SessionUtils.doMultipleWithCommit(
          () -> {
            // This is the first write in the transaction. It succeeds only if the model still has
            // the concurrency version read above, so an older request cannot overwrite a newer
            // model or add an incorrect change-log entry.
            int updated =
                SessionUtils.getWithoutCommit(
                    ModelMetaMapper.class,
                    mapper ->
                        mapper.updateModelMeta(
                            POConverters.updateModelPO(oldModelPO, newEntity), oldModelPO));
            if (updated == 0) {
              throw modelWriteFailure(identifier, oldModelPO);
            }
          },
          () -> {
            if (isRenamed) {
              SessionUtils.doWithoutCommit(
                  EntityChangeLogMapper.class,
                  mapper ->
                      mapper.insertEntityChange(
                          metalakeName,
                          Entity.EntityType.MODEL.name(),
                          oldFullName,
                          OperateType.ALTER));
            }
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.MODEL, newEntity.nameIdentifier().toString());
      throw re;
    }

    return newEntity;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchGetModelByIdentifier")
  public List<ModelEntity> batchGetModelByIdentifier(List<NameIdentifier> identifiers) {
    NameIdentifier firstIdent = identifiers.get(0);
    NameIdentifier schemaIdent = NameIdentifierUtil.getSchemaIdentifier(firstIdent);
    List<String> modelNames =
        identifiers.stream().map(NameIdentifier::name).collect(Collectors.toList());

    return SessionUtils.doWithCommitAndFetchResult(
        ModelMetaMapper.class,
        mapper -> {
          List<ModelPO> modelPOs =
              mapper.batchSelectModelByIdentifier(
                  schemaIdent.namespace().level(0),
                  schemaIdent.namespace().level(1),
                  schemaIdent.name(),
                  modelNames);
          return POConverters.fromModelPOs(modelPOs, firstIdent.namespace());
        });
  }

  /**
   * Deletes a model row only when its concurrency version has not changed since it was read.
   *
   * <p>The caller must run this method in the same transaction that removes the model's related
   * data. This allows any later cleanup failure to restore the model row as well.
   */
  void deleteModelWithVersion(NameIdentifier ident, ModelPO observedModelPO) {
    int deleted =
        SessionUtils.getWithoutCommit(
            ModelMetaMapper.class,
            mapper ->
                mapper.softDeleteModelMetaByIdAndVersion(
                    observedModelPO.getModelId(), observedModelPO.getCurrentVersion()));
    if (deleted == 0) {
      throw modelWriteFailure(ident, observedModelPO);
    }
  }

  /**
   * Advances the concurrency version shared by a model, its versions, and its aliases.
   *
   * <p>The caller uses this as the first write in a model-version transaction. It takes the model
   * row, so the model's version writes run one at a time, and a model that was dropped or renamed
   * away makes this fail before any version or alias row is modified. Version writes append to or
   * replace rows the caller already resolved, so a version somebody else registered in the meantime
   * is not a reason to reject this one; a lost update on the same row is still caught where that
   * row is written.
   */
  void bumpModelVersion(NameIdentifier ident, ModelPO observedModelPO) {
    int updated =
        SessionUtils.getWithoutCommit(
            ModelMetaMapper.class,
            mapper ->
                mapper.bumpModelVersion(
                    observedModelPO.getModelId(),
                    observedModelPO.getSchemaId(),
                    observedModelPO.getModelName()));
    if (updated == 0) {
      throw modelWriteFailure(ident, observedModelPO);
    }
  }

  /**
   * Advances the shared concurrency version and the model-version allocator in one write.
   *
   * <p>This is the model-row reservation for registering a new version. Combining both counters in
   * one statement avoids taking and updating the same row twice.
   */
  void bumpModelVersionAndLatestVersion(NameIdentifier ident, ModelPO observedModelPO) {
    int updated =
        SessionUtils.getWithoutCommit(
            ModelMetaMapper.class,
            mapper ->
                mapper.bumpModelVersionAndLatestVersion(
                    observedModelPO.getModelId(),
                    observedModelPO.getSchemaId(),
                    observedModelPO.getModelName()));
    if (updated == 0) {
      throw modelWriteFailure(ident, observedModelPO);
    }
  }

  /**
   * Explains why a version-checked model write changed no rows.
   *
   * <p>The locking read waits for a competing transaction to finish. If the same model is still at
   * the requested name, only its concurrency version changed and the caller should retry. If it was
   * deleted, renamed, or moved, the requested model no longer exists.
   */
  private RuntimeException modelWriteFailure(NameIdentifier ident, ModelPO observedModelPO) {
    ModelPO currentModelPO =
        SessionUtils.getWithoutCommit(
            ModelMetaMapper.class,
            mapper -> mapper.selectModelMetaByModelIdForUpdate(observedModelPO.getModelId()));
    if (currentModelPO == null
        || !Objects.equals(currentModelPO.getModelName(), observedModelPO.getModelName())
        || !Objects.equals(currentModelPO.getSchemaId(), observedModelPO.getSchemaId())
        || !Objects.equals(currentModelPO.getCatalogId(), observedModelPO.getCatalogId())
        || !Objects.equals(currentModelPO.getMetalakeId(), observedModelPO.getMetalakeId())) {
      return new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.MODEL.name().toLowerCase(Locale.ROOT),
          ident.toString());
    }
    return ExceptionUtils.concurrentModification(Entity.EntityType.MODEL, ident);
  }

  private void deleteModelDependents(ModelPO modelPO) {
    Long modelId = modelPO.getModelId();
    // The model row has already passed its version check. All cleanup below uses the same
    // transaction, so a failure restores both the model row and any related rows already removed.
    SessionUtils.doWithoutCommit(
        ModelVersionAliasRelMapper.class,
        mapper -> mapper.softDeleteModelVersionAliasRelsByModelId(modelId));
    SessionUtils.doWithoutCommit(
        ModelVersionMetaMapper.class, mapper -> mapper.softDeleteModelVersionsByModelId(modelId));
    SessionUtils.doWithoutCommit(
        OwnerMetaMapper.class,
        mapper ->
            mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                modelId, MetadataObject.Type.MODEL.name()));
    SessionUtils.doWithoutCommit(
        SecurableObjectMapper.class,
        mapper ->
            mapper.softDeleteObjectRelsByMetadataObject(modelId, MetadataObject.Type.MODEL.name()));
    SessionUtils.doWithoutCommit(
        TagMetadataObjectRelMapper.class,
        mapper ->
            mapper.softDeleteTagMetadataObjectRelsByMetadataObject(
                modelId, MetadataObject.Type.MODEL.name()));
    SessionUtils.doWithoutCommit(
        StatisticMetaMapper.class, mapper -> mapper.softDeleteStatisticsByEntityId(modelId));
    SessionUtils.doWithoutCommit(
        PolicyMetadataObjectRelMapper.class,
        mapper ->
            mapper.softDeletePolicyMetadataObjectRelsByMetadataObject(
                modelId, MetadataObject.Type.MODEL.name()));
  }
}
