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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionAliasRelMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionMetaMapper;
import org.apache.gravitino.storage.relational.po.ModelPO;
import org.apache.gravitino.storage.relational.po.ModelVersionAliasRelPO;
import org.apache.gravitino.storage.relational.po.ModelVersionPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelVersionMetaService {

  private static final Logger LOG = LoggerFactory.getLogger(ModelVersionMetaService.class);

  private static final ModelVersionMetaService INSTANCE = new ModelVersionMetaService();

  public static ModelVersionMetaService getInstance() {
    return INSTANCE;
  }

  private ModelVersionMetaService() {}

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listModelVersionsByNamespace")
  public List<ModelVersionEntity> listModelVersionsByNamespace(Namespace ns) {
    NamespaceUtil.checkModelVersion(ns);

    NameIdentifier modelIdent = NameIdentifier.of(ns.levels());
    // Will throw a NoSuchEntityException if the model does not exist.
    ModelEntity modelEntity = ModelMetaService.getInstance().getModelByIdentifier(modelIdent);

    List<ModelVersionPO> modelVersionPOs =
        SessionUtils.getWithoutCommit(
            ModelVersionMetaMapper.class,
            mapper -> mapper.listModelVersionMetasByModelId(modelEntity.id()));

    if (modelVersionPOs.isEmpty()) {
      return Collections.emptyList();
    }

    // Get the aliases for all the model versions.
    List<ModelVersionAliasRelPO> aliasRelPOs =
        SessionUtils.getWithoutCommit(
            ModelVersionAliasRelMapper.class,
            mapper -> mapper.selectModelVersionAliasRelsByModelId(modelEntity.id()));
    Multimap<Integer, ModelVersionAliasRelPO> aliasRelPOsByModelVersion =
        ArrayListMultimap.create();
    aliasRelPOs.forEach(r -> aliasRelPOsByModelVersion.put(r.getModelVersion(), r));

    return ImmutableList.copyOf(
        modelVersionPOs.stream()
            .collect(
                Collectors.groupingBy(
                    ModelVersionPO::getModelVersion,
                    Collectors.collectingAndThen(
                        Collectors.<ModelVersionPO>toList(),
                        m -> {
                          List<ModelVersionAliasRelPO> versionAliasRelPOs =
                              Lists.newArrayList(
                                  aliasRelPOsByModelVersion.get(m.get(0).getModelVersion()));
                          return POConverters.fromModelVersionPO(modelIdent, m, versionAliasRelPOs);
                        })))
            .values());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getModelVersionByIdentifier")
  public ModelVersionEntity getModelVersionByIdentifier(NameIdentifier ident) {
    NameIdentifierUtil.checkModelVersion(ident);

    NameIdentifier modelIdent = NameIdentifier.of(ident.namespace().levels());
    // Will throw a NoSuchEntityException if the model does not exist.
    ModelEntity modelEntity = ModelMetaService.getInstance().getModelByIdentifier(modelIdent);

    boolean isVersionNumber = NumberUtils.isCreatable(ident.name());

    List<ModelVersionPO> modelVersionPOs =
        SessionUtils.getWithoutCommit(
            ModelVersionMetaMapper.class,
            mapper -> {
              if (isVersionNumber) {
                return mapper.selectModelVersionMeta(
                    modelEntity.id(), Integer.valueOf(ident.name()));
              } else {
                return mapper.selectModelVersionMetaByAlias(modelEntity.id(), ident.name());
              }
            });

    if (modelVersionPOs.isEmpty()) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.MODEL_VERSION.name().toLowerCase(Locale.ROOT),
          ident.toString());
    }

    List<ModelVersionAliasRelPO> aliasRelPOs =
        SessionUtils.getWithoutCommit(
            ModelVersionAliasRelMapper.class,
            mapper -> {
              if (isVersionNumber) {
                return mapper.selectModelVersionAliasRelsByModelIdAndVersion(
                    modelEntity.id(), Integer.valueOf(ident.name()));
              } else {
                return mapper.selectModelVersionAliasRelsByModelIdAndAlias(
                    modelEntity.id(), ident.name());
              }
            });

    return POConverters.fromModelVersionPO(modelIdent, modelVersionPOs, aliasRelPOs);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertModelVersion")
  public void insertModelVersion(ModelVersionEntity modelVersionEntity) throws IOException {
    NameIdentifier modelIdent = modelVersionEntity.modelIdentifier();
    NameIdentifierUtil.checkModel(modelIdent);

    ModelPO modelPO = ModelMetaService.getInstance().getModelPOByIdentifier(modelIdent);
    Long modelId = modelPO.getModelId();

    List<ModelVersionPO> modelVersionPOs =
        POConverters.initializeModelVersionPO(modelVersionEntity, modelId);
    List<ModelVersionAliasRelPO> aliasRelPOs =
        POConverters.initializeModelVersionAliasRelPO(modelVersionEntity, modelId);

    try {
      SessionUtils.doMultipleWithCommit(
          // Keep the schema locked while adding the version. Otherwise a concurrent schema delete
          // could finish its cleanup just before these new rows are inserted.
          () -> lockSchemaForModelVersionWrite(modelIdent, modelPO),
          // Advance the concurrency version shared by the model and all its versions before
          // inserting URI or alias rows. A competing writer makes this check fail before any
          // partial data is added.
          () -> ModelMetaService.getInstance().bumpModelVersion(modelIdent, modelPO),
          () ->
              SessionUtils.doWithoutCommit(
                  ModelVersionMetaMapper.class,
                  mapper -> mapper.insertModelVersionMetas(modelVersionPOs)),
          () -> {
            if (aliasRelPOs.isEmpty()) {
              return;
            }
            SessionUtils.doWithoutCommit(
                ModelVersionAliasRelMapper.class,
                mapper -> mapper.insertModelVersionAliasRels(aliasRelPOs));
          },
          () -> {
            // The insert statements above use the old model_latest_version as the number of the new
            // version. Increment it only after those inserts. The earlier version check keeps the
            // model row locked, so another registration cannot choose the same number.
            int updated =
                SessionUtils.getWithoutCommit(
                    ModelMetaMapper.class, mapper -> mapper.updateModelLatestVersion(modelId));
            if (updated == 0) {
              throw noSuchModelException(modelIdent);
            }
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.MODEL_VERSION, modelVersionEntity.modelIdentifier().toString());
      throw re;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteModelVersion")
  public boolean deleteModelVersion(NameIdentifier ident) {
    NameIdentifierUtil.checkModelVersion(ident);

    NameIdentifier modelIdent = NameIdentifier.of(ident.namespace().levels());
    ModelPO modelPO;
    try {
      modelPO = ModelMetaService.getInstance().getModelPOByIdentifier(modelIdent);
    } catch (NoSuchEntityException e) {
      return false;
    }

    boolean isVersionNumber = NumberUtils.isCreatable(ident.name());
    // Resolve an alias to its numeric model-version value once. The concurrency-version check
    // below will fail if another writer changes the version or alias after this read.
    List<ModelVersionPO> observedVersionPOs =
        SessionUtils.getWithoutCommit(
            ModelVersionMetaMapper.class,
            mapper -> {
              if (isVersionNumber) {
                return mapper.selectModelVersionMeta(
                    modelPO.getModelId(), Integer.valueOf(ident.name()));
              }
              return mapper.selectModelVersionMetaByAlias(modelPO.getModelId(), ident.name());
            });
    if (observedVersionPOs.isEmpty()) {
      return false;
    }
    Integer modelVersion = observedVersionPOs.get(0).getModelVersion();

    try {
      SessionUtils.doMultipleWithCommit(
          // Keep the parent schema from being deleted while this transaction changes version rows.
          () -> lockSchemaForModelVersionWrite(modelIdent, modelPO),
          // Reserve this write by advancing the shared concurrency version before deleting
          // anything. If the model changed after the read above, leave the version and its aliases
          // untouched.
          () -> ModelMetaService.getInstance().bumpModelVersion(modelIdent, modelPO),
          () -> {
            // An alias was resolved to its numeric version above. Delete every URI row belonging to
            // that version, regardless of whether the caller supplied the number or an alias.
            int deleted =
                SessionUtils.getWithoutCommit(
                    ModelVersionMetaMapper.class,
                    mapper ->
                        mapper.softDeleteModelVersionMetaByModelIdAndVersion(
                            modelPO.getModelId(), modelVersion));
            if (deleted == 0) {
              throw noSuchModelVersionException(ident);
            }
          },
          // Remove all aliases for the same numeric version in the same transaction.
          () ->
              SessionUtils.doWithoutCommit(
                  ModelVersionAliasRelMapper.class,
                  mapper ->
                      mapper.softDeleteModelVersionAliasRelsByModelIdAndVersion(
                          modelPO.getModelId(), modelVersion)));
    } catch (NoSuchEntityException e) {
      // The model, its parent schema or the version itself was dropped between the reads above and
      // this transaction. Both reads report that case by returning false, so a delete that races
      // with another delete keeps returning false instead of surfacing as an error.
      LOG.warn("Failed to delete model version: {}", ident, e);
      return false;
    }

    return true;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteModelVersionMetasByLegacyTimeline")
  public int deleteModelVersionMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    int[] modelVersionDeletedCount = new int[] {0};
    int[] modelVersionAliasRelDeletedCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            modelVersionDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    ModelVersionMetaMapper.class,
                    mapper ->
                        mapper.deleteModelVersionMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            modelVersionAliasRelDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    ModelVersionAliasRelMapper.class,
                    mapper ->
                        mapper.deleteModelVersionAliasRelsByLegacyTimeline(legacyTimeline, limit)));

    return modelVersionDeletedCount[0] + modelVersionAliasRelDeletedCount[0];
  }

  /**
   * Updates the model version entity.
   *
   * @param ident the {@link NameIdentifier} instance of the model version to update
   * @param updater the function to update the model version entity
   * @return the updated model version entity
   * @param <E> the type of the entity to update
   * @throws IOException if an error occurs while updating the entity
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateModelVersion")
  public <E extends Entity & HasIdentifier> ModelVersionEntity updateModelVersion(
      NameIdentifier ident, Function<E, E> updater) throws IOException {
    NameIdentifierUtil.checkModelVersion(ident);
    NameIdentifier modelIdent = NameIdentifier.of(ident.namespace().levels());

    boolean isVersionNumber = NumberUtils.isCreatable(ident.name());
    ModelPO modelPO = ModelMetaService.getInstance().getModelPOByIdentifier(modelIdent);
    Long modelId = modelPO.getModelId();

    List<ModelVersionPO> oldModelVersionPOs =
        SessionUtils.getWithoutCommit(
            ModelVersionMetaMapper.class,
            mapper -> {
              if (isVersionNumber) {
                return mapper.selectModelVersionMeta(modelId, Integer.valueOf(ident.name()));
              } else {
                return mapper.selectModelVersionMetaByAlias(modelId, ident.name());
              }
            });

    if (oldModelVersionPOs.isEmpty()) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.MODEL_VERSION.name().toLowerCase(Locale.ROOT),
          ident.toString());
    }

    List<ModelVersionAliasRelPO> oldAliasRelPOs =
        SessionUtils.getWithoutCommit(
            ModelVersionAliasRelMapper.class,
            mapper -> {
              if (isVersionNumber) {
                return mapper.selectModelVersionAliasRelsByModelIdAndVersion(
                    modelId, Integer.valueOf(ident.name()));
              } else {
                return mapper.selectModelVersionAliasRelsByModelIdAndAlias(modelId, ident.name());
              }
            });

    ModelVersionEntity oldModelVersionEntity =
        POConverters.fromModelVersionPO(modelIdent, oldModelVersionPOs, oldAliasRelPOs);
    ModelVersionEntity newModelVersionEntity =
        (ModelVersionEntity) updater.apply((E) oldModelVersionEntity);

    Preconditions.checkArgument(
        Objects.equals(oldModelVersionEntity.version(), newModelVersionEntity.version()),
        "The updated model version: %s should be same with the table entity version before: %s",
        newModelVersionEntity.version(),
        oldModelVersionEntity.version());

    boolean isAliasChanged =
        isModelVersionAliasUpdated(oldModelVersionEntity, newModelVersionEntity);
    List<ModelVersionAliasRelPO> newAliasRelPOs =
        POConverters.updateModelVersionAliasRelPO(oldAliasRelPOs, newModelVersionEntity, modelId);

    boolean isModelVersionUriUpdated =
        isModelVersionUriUpdated(oldModelVersionEntity, newModelVersionEntity);

    try {
      SessionUtils.doMultipleWithCommit(
          // Keep the schema locked because URI and alias changes may replace active child rows.
          () -> lockSchemaForModelVersionWrite(modelIdent, modelPO),
          // Advance the shared concurrency version before changing child rows. A competing model
          // or model-version writer then makes this operation fail without leaving partial changes.
          () -> ModelMetaService.getInstance().bumpModelVersion(modelIdent, modelPO),
          () -> {
            int updated;
            if (isModelVersionUriUpdated) {
              // URI rows share the same model-version number. Replace the complete set so URI
              // names removed by the update do not remain active.
              updated =
                  SessionUtils.getWithoutCommit(
                      ModelVersionMetaMapper.class,
                      mapper ->
                          mapper.softDeleteModelVersionMetaByModelIdAndVersion(
                              modelId, oldModelVersionPOs.get(0).getModelVersion()));

              List<ModelVersionPO> modelVersionPOs =
                  POConverters.initializeModelVersionPO(newModelVersionEntity, modelId);
              SessionUtils.doWithoutCommit(
                  ModelVersionMetaMapper.class,
                  mapper -> mapper.insertModelVersionMetasWithVersionNumber(modelVersionPOs));
            } else {
              // When the URI set is unchanged, update the common fields in place.
              updated =
                  SessionUtils.getWithoutCommit(
                      ModelVersionMetaMapper.class,
                      mapper ->
                          mapper.updateModelVersionMeta(
                              POConverters.updateModelVersionPO(
                                  oldModelVersionPOs.get(0), newModelVersionEntity),
                              oldModelVersionPOs.get(0)));
            }
            if (updated == 0) {
              // The row this update was built from is no longer there. Throwing here also rolls
              // back the shared concurrency-version change made above.
              throw modelVersionWriteFailure(ident, modelId, oldModelVersionPOs.get(0));
            }
          },
          () -> {
            if (isAliasChanged) {
              // Replace the full alias set by numeric version. This also works when the request
              // identified the version through one of its aliases.
              SessionUtils.doWithoutCommit(
                  ModelVersionAliasRelMapper.class,
                  mapper ->
                      mapper.softDeleteModelVersionAliasRelsByModelIdAndVersion(
                          modelId, oldModelVersionEntity.version()));

              if (!newAliasRelPOs.isEmpty()) {
                // An empty list means that the update removed every alias; there is nothing to
                // insert after the old aliases have been deleted.
                SessionUtils.doWithoutCommit(
                    ModelVersionAliasRelMapper.class,
                    mapper -> mapper.updateModelVersionAliasRel(newAliasRelPOs));
              }
            }
          });

    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.MODEL_VERSION, newModelVersionEntity.nameIdentifier().toString());
      throw re;
    }

    return newModelVersionEntity;
  }

  private boolean isModelVersionAliasUpdated(
      ModelVersionEntity oldModelVersionEntity, ModelVersionEntity newModelVersionEntity) {
    List<String> oldAliases = oldModelVersionEntity.aliases();
    List<String> newAliases = newModelVersionEntity.aliases();

    if (oldAliases.size() != newAliases.size()) {
      return true;
    }

    return !oldAliases.equals(newAliases);
  }

  private boolean isModelVersionUriUpdated(
      ModelVersionEntity oldModelVersionEntity, ModelVersionEntity newModelVersionEntity) {
    Map<String, String> oldUris = oldModelVersionEntity.uris();
    Map<String, String> newUris = newModelVersionEntity.uris();
    return !oldUris.equals(newUris);
  }

  private void lockSchemaForModelVersionWrite(
      NameIdentifier modelIdentifier, ModelPO observedModelPO) {
    SchemaMetaService.getInstance()
        .lockSchemaForEntityWrite(
            modelIdentifier,
            observedModelPO.getSchemaId(),
            observedModelPO.getCatalogId(),
            observedModelPO.getMetalakeId());
  }

  private NoSuchEntityException noSuchModelException(NameIdentifier modelIdentifier) {
    return new NoSuchEntityException(
        NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
        Entity.EntityType.MODEL.name().toLowerCase(Locale.ROOT),
        modelIdentifier.toString());
  }

  /**
   * Decides which error a model-version write that matched no row should report.
   *
   * <p>The write compares the values it read, so it matches nothing either because the version was
   * deleted, which is a missing entity, or because somebody else changed the same version first,
   * which is a conflict.
   */
  private RuntimeException modelVersionWriteFailure(
      NameIdentifier ident, Long modelId, ModelVersionPO observedVersionPO) {
    List<ModelVersionPO> currentVersionPOs =
        SessionUtils.getWithoutCommit(
            ModelVersionMetaMapper.class,
            mapper -> mapper.selectModelVersionMeta(modelId, observedVersionPO.getModelVersion()));
    if (currentVersionPOs.isEmpty()) {
      return noSuchModelVersionException(ident);
    }
    return ExceptionUtils.concurrentModification(Entity.EntityType.MODEL_VERSION, ident);
  }

  private NoSuchEntityException noSuchModelVersionException(NameIdentifier modelVersionIdentifier) {
    return new NoSuchEntityException(
        NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
        Entity.EntityType.MODEL_VERSION.name().toLowerCase(Locale.ROOT),
        modelVersionIdentifier.toString());
  }
}
