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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.gravitino.Entity;
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
import org.glassfish.jersey.internal.guava.Lists;

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

    return modelVersionPOs.stream()
        .map(
            m -> {
              List<ModelVersionAliasRelPO> versionAliasRelPOs =
                  Lists.newArrayList(aliasRelPOsByModelVersion.get(m.getModelVersion()));
              return POConverters.fromModelVersionPO(modelIdent, m, versionAliasRelPOs);
            })
        .collect(Collectors.toList());
  }

  public ModelVersionEntity getModelVersionByIdentifier(NameIdentifier ident) {
    NameIdentifierUtil.checkModelVersion(ident);

    NameIdentifier modelIdent = NameIdentifier.of(ident.namespace().levels());
    // Will throw a NoSuchEntityException if the model does not exist.
    ModelEntity modelEntity = ModelMetaService.getInstance().getModelByIdentifier(modelIdent);

    boolean isVersionNumber = NumberUtils.isCreatable(ident.name());

    ModelVersionPO modelVersionPO =
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

    if (modelVersionPO == null) {
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

    return POConverters.fromModelVersionPO(modelIdent, modelVersionPO, aliasRelPOs);
  }

  public void insertModelVersion(ModelVersionEntity modelVersionEntity) throws IOException {
    NameIdentifier modelIdent = modelVersionEntity.modelIdentifier();
    NameIdentifierUtil.checkModel(modelIdent);

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(modelIdent.namespace());
    Long modelId =
        ModelMetaService.getInstance()
            .getModelIdBySchemaIdAndModelName(schemaId, modelIdent.name());

    ModelVersionPO.Builder builder = ModelVersionPO.builder().withModelId(modelId);
    ModelVersionPO modelVersionPO =
        POConverters.initializeModelVersionPO(modelVersionEntity, builder);
    List<ModelVersionAliasRelPO> aliasRelPOs =
        POConverters.initializeModelVersionAliasRelPO(modelVersionEntity, modelId);

    try {
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  ModelVersionMetaMapper.class,
                  mapper -> mapper.insertModelVersionMeta(modelVersionPO)),
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
                SessionUtils.doWithoutCommitAndFetchResult(
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
                SessionUtils.doWithoutCommitAndFetchResult(
                    ModelVersionMetaMapper.class,
                    mapper ->
                        mapper.deleteModelVersionMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            modelVersionAliasRelDeletedCount[0] =
                SessionUtils.doWithoutCommitAndFetchResult(
                    ModelVersionAliasRelMapper.class,
                    mapper ->
                        mapper.deleteModelVersionAliasRelsByLegacyTimeline(legacyTimeline, limit)));

    return modelVersionDeletedCount[0] + modelVersionAliasRelDeletedCount[0];
  }
}
