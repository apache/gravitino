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
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionAliasRelMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionMetaMapper;
import org.apache.gravitino.storage.relational.po.ModelVersionAliasRelPO;
import org.apache.gravitino.storage.relational.po.ModelVersionPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

public class ModelVersionMetaService {

  private static final ModelVersionMetaService INSTANCE = new ModelVersionMetaService();

  public static ModelVersionMetaService getInstance() {
    return INSTANCE;
  }

  private ModelVersionMetaService() {}

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

  public void insertModelVersion(ModelVersionEntity modelVersionEntity) throws IOException {
    NameIdentifier modelIdent = modelVersionEntity.modelIdentifier();
    NameIdentifierUtil.checkModel(modelIdent);

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(modelIdent.namespace());
    Long modelId =
        ModelMetaService.getInstance()
            .getModelIdBySchemaIdAndModelName(schemaId, modelIdent.name());

    List<ModelVersionPO> modelVersionPOs =
        POConverters.initializeModelVersionPO(modelVersionEntity, modelId);
    List<ModelVersionAliasRelPO> aliasRelPOs =
        POConverters.initializeModelVersionAliasRelPO(modelVersionEntity, modelId);

    try {
      SessionUtils.doMultipleWithCommit(
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
          () ->
              // If the model version is inserted successfully, update the model latest version.
              SessionUtils.doWithoutCommit(
                  ModelMetaMapper.class, mapper -> mapper.updateModelLatestVersion(modelId)));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.MODEL_VERSION, modelVersionEntity.modelIdentifier().toString());
      throw re;
    }
  }

  public boolean deleteModelVersion(NameIdentifier ident) {
    NameIdentifierUtil.checkModelVersion(ident);

    NameIdentifier modelIdent = NameIdentifier.of(ident.namespace().levels());
    // Will throw a NoSuchEntityException if the model does not exist.
    ModelEntity modelEntity;
    try {
      modelEntity = ModelMetaService.getInstance().getModelByIdentifier(modelIdent);
    } catch (NoSuchEntityException e) {
      return false;
    }

    boolean isVersionNumber = NumberUtils.isCreatable(ident.name());

    AtomicInteger modelVersionDeletedCount = new AtomicInteger();
    SessionUtils.doMultipleWithCommit(
        // Delete model version relations first
        () ->
            modelVersionDeletedCount.set(
                SessionUtils.getWithoutCommit(
                    ModelVersionMetaMapper.class,
                    mapper -> {
                      if (isVersionNumber) {
                        return mapper.softDeleteModelVersionMetaByModelIdAndVersion(
                            modelEntity.id(), Integer.valueOf(ident.name()));
                      } else {
                        return mapper.softDeleteModelVersionMetaByModelIdAndAlias(
                            modelEntity.id(), ident.name());
                      }
                    })),
        () -> {
          // Delete model version alias relations
          if (modelVersionDeletedCount.get() == 0) {
            return;
          }

          SessionUtils.doWithoutCommit(
              ModelVersionAliasRelMapper.class,
              mapper -> {
                if (isVersionNumber) {
                  mapper.softDeleteModelVersionAliasRelsByModelIdAndVersion(
                      modelEntity.id(), Integer.valueOf(ident.name()));
                } else {
                  mapper.softDeleteModelVersionAliasRelsByModelIdAndAlias(
                      modelEntity.id(), ident.name());
                }
              });
        });

    return modelVersionDeletedCount.get() > 0;
  }

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
  public <E extends Entity & HasIdentifier> ModelVersionEntity updateModelVersion(
      NameIdentifier ident, Function<E, E> updater) throws IOException {
    NameIdentifierUtil.checkModelVersion(ident);
    NameIdentifier modelIdent = NameIdentifier.of(ident.namespace().levels());

    boolean isVersionNumber = NumberUtils.isCreatable(ident.name());
    ModelEntity modelEntity = ModelMetaService.getInstance().getModelByIdentifier(modelIdent);

    List<ModelVersionPO> oldModelVersionPOs =
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
                    modelEntity.id(), Integer.valueOf(ident.name()));
              } else {
                return mapper.selectModelVersionAliasRelsByModelIdAndAlias(
                    modelEntity.id(), ident.name());
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
        POConverters.updateModelVersionAliasRelPO(oldAliasRelPOs, newModelVersionEntity);

    boolean isModelVersionUriUpdated =
        isModelVersionUriUpdated(oldModelVersionEntity, newModelVersionEntity);

    final AtomicInteger updateResult = new AtomicInteger(0);
    try {
      SessionUtils.doMultipleWithCommit(
          () -> {
            if (isModelVersionUriUpdated) {
              // delete old model version POs first
              updateResult.addAndGet(
                  SessionUtils.getWithoutCommit(
                      ModelVersionMetaMapper.class,
                      mapper -> {
                        if (isVersionNumber) {
                          return mapper.softDeleteModelVersionMetaByModelIdAndVersion(
                              modelEntity.id(), Integer.valueOf(ident.name()));
                        } else {
                          return mapper.softDeleteModelVersionMetaByModelIdAndAlias(
                              modelEntity.id(), ident.name());
                        }
                      }));

              // insert model version POs with updated URIs
              List<ModelVersionPO> modelVersionPOs =
                  POConverters.initializeModelVersionPO(newModelVersionEntity, modelEntity.id());
              SessionUtils.doWithoutCommit(
                  ModelVersionMetaMapper.class,
                  mapper -> mapper.insertModelVersionMetasWithVersionNumber(modelVersionPOs));
            } else {
              // update model version POs directly
              updateResult.addAndGet(
                  SessionUtils.getWithoutCommit(
                      ModelVersionMetaMapper.class,
                      mapper ->
                          mapper.updateModelVersionMeta(
                              POConverters.updateModelVersionPO(
                                  oldModelVersionPOs.get(0), newModelVersionEntity),
                              oldModelVersionPOs.get(0))));
            }
          },
          () -> {
            if (isAliasChanged) {
              SessionUtils.doWithoutCommit(
                  ModelVersionAliasRelMapper.class,
                  mapper ->
                      oldModelVersionEntity
                          .aliases()
                          .forEach(
                              alias ->
                                  mapper.softDeleteModelVersionAliasRelsByModelIdAndAlias(
                                      modelEntity.id(), alias)));

              SessionUtils.doWithoutCommit(
                  ModelVersionAliasRelMapper.class,
                  mapper -> mapper.updateModelVersionAliasRel(newAliasRelPOs));
            }
          });

    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.CATALOG, newModelVersionEntity.nameIdentifier().toString());
      throw re;
    }

    if (updateResult.get() > 0) {
      return newModelVersionEntity;
    } else {
      throw new IOException("Failed to update the entity: " + ident);
    }
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
}
