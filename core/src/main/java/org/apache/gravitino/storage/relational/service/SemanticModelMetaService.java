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
import static org.apache.gravitino.storage.relational.po.SemanticModelPO.fromSemanticModelPO;
import static org.apache.gravitino.storage.relational.po.SemanticModelPO.initializeSemanticModelPO;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelVersionInfoMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelPO;
import org.apache.gravitino.storage.relational.po.SemanticModelVersionInfoPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Provides relational create and load operations for Semantic Model metadata. */
public class SemanticModelMetaService {

  private static final SemanticModelMetaService INSTANCE = new SemanticModelMetaService();

  private final BasePOStorageOps<SemanticModelPO, SemanticModelMetaMapper> ops;

  /** Returns the singleton Semantic Model metadata service. */
  public static SemanticModelMetaService getInstance() {
    return INSTANCE;
  }

  private SemanticModelMetaService() {
    this.ops = new HierarchicalConversionPOStorageOps<>(new SemanticModelPOStorageOps());
  }

  /**
   * Resolves a Semantic Model stable ID by schema ID and name.
   *
   * @param schemaId The parent schema ID.
   * @param semanticModelName The Semantic Model name.
   * @return The stable Semantic Model ID.
   * @throws NoSuchEntityException If the Semantic Model does not exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getSemanticModelIdBySchemaIdAndName")
  public Long getSemanticModelIdBySchemaIdAndName(long schemaId, String semanticModelName) {
    Long semanticModelId =
        SessionUtils.getWithoutCommit(
            SemanticModelMetaMapper.class,
            mapper -> mapper.selectSemanticModelIdBySchemaIdAndName(schemaId, semanticModelName));
    if (semanticModelId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SEMANTIC_MODEL.name().toLowerCase(Locale.ROOT),
          semanticModelName);
    }
    return semanticModelId;
  }

  /**
   * Loads the current Semantic Model by identifier.
   *
   * @param identifier The Semantic Model identifier.
   * @return The current Semantic Model entity.
   * @throws NoSuchEntityException If the Semantic Model does not exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getSemanticModelByIdentifier")
  public SemanticModelEntity getSemanticModelByIdentifier(NameIdentifier identifier) {
    SemanticModelPO semanticModelPO = getSemanticModelPOByIdentifier(identifier);
    return fromSemanticModelPO(semanticModelPO, identifier.namespace());
  }

  /**
   * Inserts or overwrites a Semantic Model identity and its snapshot atomically.
   *
   * @param semanticModelEntity The Semantic Model entity.
   * @param overwrite Whether to overwrite rows for the same stable ID or natural key.
   * @throws IOException If relational persistence fails.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertSemanticModel")
  public void insertSemanticModel(SemanticModelEntity semanticModelEntity, boolean overwrite)
      throws IOException {
    NameIdentifierUtil.checkSemanticModel(semanticModelEntity.nameIdentifier());
    try {
      SemanticModelPO po =
          initializeSemanticModelPO(semanticModelEntity, SemanticModelPO.builder());
      AtomicReference<SemanticModelPO> persistedPO = new AtomicReference<>(po);
      SessionUtils.doMultipleWithCommit(
          () ->
              SchemaMetaService.getInstance()
                  .lockSchemaForEntityWrite(
                      semanticModelEntity.nameIdentifier(),
                      po.getSchemaId(),
                      po.getCatalogId(),
                      po.getMetalakeId()),
          () ->
              SessionUtils.doWithoutCommit(
                  SemanticModelMetaMapper.class,
                  mapper -> {
                    SemanticModelPO storedPO =
                        overwrite
                            ? mapper.selectSemanticModelMetaBySchemaIdAndNameForUpdate(
                                po.getSchemaId(), po.getSemanticModelName())
                            : null;
                    if (storedPO == null) {
                      // Keep a missing-row import strict. Concurrent imports that both observed no
                      // identity must not turn the losing insert into an overwrite with another ID.
                      ops.insertPO(mapper, po, false);
                      return;
                    }

                    SemanticModelPO replacementPO = semanticModelForOverwrite(po, storedPO);
                    Integer updated = mapper.updateSemanticModelMeta(replacementPO, storedPO);
                    Preconditions.checkState(
                        updated != null && updated == 1,
                        "The overwritten Semantic Model %s in schema %s changed while its row was held",
                        po.getSemanticModelName(),
                        po.getSchemaId());
                    persistedPO.set(replacementPO);
                  }),
          () ->
              SessionUtils.doWithoutCommit(
                  SemanticModelVersionInfoMapper.class,
                  mapper ->
                      mapper.insertSemanticModelVersionInfo(
                          persistedPO.get().getSemanticModelVersionInfoPO())));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.SEMANTIC_MODEL, semanticModelEntity.nameIdentifier().toString());
      throw re;
    }
  }

  /** Returns the persistent-object operations used by this service. */
  public BasePOStorageOps<SemanticModelPO, SemanticModelMetaMapper> ops() {
    return ops;
  }

  private static SemanticModelPO semanticModelForOverwrite(
      SemanticModelPO source, SemanticModelPO persistedPO) {
    int previousVersion = Math.max(persistedPO.getCurrentVersion(), persistedPO.getLastVersion());
    Preconditions.checkState(
        previousVersion < Integer.MAX_VALUE,
        "Semantic Model %s has exhausted the version range",
        persistedPO.getSemanticModelId());
    int nextVersion = previousVersion + 1;
    return SemanticModelPO.builder()
        .withSemanticModelId(persistedPO.getSemanticModelId())
        .withSemanticModelName(source.getSemanticModelName())
        .withMetalakeId(persistedPO.getMetalakeId())
        .withCatalogId(persistedPO.getCatalogId())
        .withSchemaId(persistedPO.getSchemaId())
        .withAuditInfo(source.getAuditInfo())
        .withCurrentVersion(nextVersion)
        .withLastVersion(nextVersion)
        .withDeletedAt(source.getDeletedAt())
        .withSemanticModelVersionInfoPO(
            versionInfoForOverwrite(
                source.getSemanticModelVersionInfoPO(), persistedPO, nextVersion))
        .build();
  }

  private static SemanticModelVersionInfoPO versionInfoForOverwrite(
      SemanticModelVersionInfoPO source, SemanticModelPO persistedPO, int nextVersion) {
    return SemanticModelVersionInfoPO.builder()
        .withMetalakeId(persistedPO.getMetalakeId())
        .withCatalogId(persistedPO.getCatalogId())
        .withSchemaId(persistedPO.getSchemaId())
        .withSemanticModelId(persistedPO.getSemanticModelId())
        .withVersion(nextVersion)
        .withSemanticModelName(source.semanticModelName())
        .withSemanticModelComment(source.semanticModelComment())
        .withSemanticModelDefinition(source.semanticModelDefinition())
        .withProperties(source.properties())
        .withAuditInfo(source.auditInfo())
        .withDeletedAt(source.deletedAt())
        .build();
  }

  private SemanticModelPO getSemanticModelPOByIdentifier(NameIdentifier identifier) {
    NameIdentifierUtil.checkSemanticModel(identifier);
    SemanticModelPO semanticModelPO =
        SessionUtils.getWithoutCommit(
            SemanticModelMetaMapper.class,
            mapper ->
                POStorageReadRouting.getPO(
                    mapper, identifier, ops, Entity.EntityType.SEMANTIC_MODEL));
    if (semanticModelPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SEMANTIC_MODEL.name().toLowerCase(Locale.ROOT),
          identifier.name());
    }
    return semanticModelPO;
  }
}
