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
import static org.apache.gravitino.storage.relational.po.FunctionPO.buildFunctionPO;
import static org.apache.gravitino.storage.relational.po.FunctionPO.fromFunctionPO;
import static org.apache.gravitino.storage.relational.po.FunctionPO.initializeFunctionPO;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.FunctionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FunctionVersionMetaMapper;
import org.apache.gravitino.storage.relational.po.FunctionMaxVersionPO;
import org.apache.gravitino.storage.relational.po.FunctionPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FunctionMetaService {
  public static FunctionMetaService getInstance() {
    return INSTANCE;
  }

  private static final Logger LOG = LoggerFactory.getLogger(FunctionMetaService.class);
  private static final FunctionMetaService INSTANCE = new FunctionMetaService();

  private FunctionMetaService() {}

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listFunctionsByNamespace")
  public List<FunctionEntity> listFunctionsByNamespace(Namespace ns) {
    NamespaceUtil.checkFunction(ns);

    List<FunctionPO> functionPOs = listFunctionPOs(ns);
    return functionPOs.stream().map(f -> fromFunctionPO(f, ns)).collect(Collectors.toList());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getFunctionByIdentifier")
  public FunctionEntity getFunctionByIdentifier(NameIdentifier ident) {
    FunctionPO functionPO = getFunctionPOByIdentifier(ident);
    return fromFunctionPO(functionPO, ident.namespace());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertFunction")
  public void insertFunction(FunctionEntity functionEntity, boolean overwrite) throws IOException {
    NameIdentifierUtil.checkFunction(functionEntity.nameIdentifier());

    FunctionPO.FunctionPOBuilder builder = FunctionPO.builder();
    try {
      fillFunctionPOBuilderParentEntityId(builder, functionEntity.namespace());
      FunctionPO po = initializeFunctionPO(functionEntity, builder);

      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  FunctionMetaMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertFunctionMetaOnDuplicateKeyUpdate(po);
                    } else {
                      mapper.insertFunctionMeta(po);
                    }
                  }),
          () ->
              SessionUtils.doWithoutCommit(
                  FunctionVersionMetaMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertFunctionVersionMetaOnDuplicateKeyUpdate(po.functionVersionPO());
                    } else {
                      mapper.insertFunctionVersionMeta(po.functionVersionPO());
                    }
                  }));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.FUNCTION, functionEntity.nameIdentifier().toString());
      throw re;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteFunction")
  public boolean deleteFunction(NameIdentifier ident) {
    FunctionPO functionPO = getFunctionPOByIdentifier(ident);
    Long functionId = functionPO.functionId();

    AtomicInteger functionDeletedCount = new AtomicInteger();
    SessionUtils.doMultipleWithCommit(
        // delete function meta
        () ->
            functionDeletedCount.set(
                SessionUtils.getWithoutCommit(
                    FunctionMetaMapper.class,
                    mapper -> mapper.softDeleteFunctionMetaByFunctionId(functionId))),

        // delete function versions first
        () -> {
          if (functionDeletedCount.get() > 0) {
            SessionUtils.doWithoutCommit(
                FunctionVersionMetaMapper.class,
                mapper -> mapper.softDeleteFunctionVersionsByFunctionId(functionId));
          }
        });

    return functionDeletedCount.get() > 0;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteFunctionMetasByLegacyTimeline")
  public int deleteFunctionMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    int functionDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            FunctionVersionMetaMapper.class,
            mapper -> mapper.deleteFunctionVersionMetasByLegacyTimeline(legacyTimeline, limit));

    int functionVersionDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            FunctionMetaMapper.class,
            mapper -> mapper.deleteFunctionMetasByLegacyTimeline(legacyTimeline, limit));

    return functionDeletedCount + functionVersionDeletedCount;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteFunctionVersionsByRetentionCount")
  public int deleteFunctionVersionsByRetentionCount(Long versionRetentionCount, int limit) {
    List<FunctionMaxVersionPO> functionCurVersions =
        SessionUtils.getWithoutCommit(
            FunctionVersionMetaMapper.class,
            mapper -> mapper.selectFunctionVersionsByRetentionCount(versionRetentionCount));

    int totalDeletedCount = 0;
    for (FunctionMaxVersionPO functionCurVersion : functionCurVersions) {
      long versionRetentionLine = functionCurVersion.version() - versionRetentionCount;
      int deletedCount =
          SessionUtils.doWithCommitAndFetchResult(
              FunctionVersionMetaMapper.class,
              mapper ->
                  mapper.softDeleteFunctionVersionsByRetentionLine(
                      functionCurVersion.functionId(), versionRetentionLine, limit));
      totalDeletedCount += deletedCount;

      LOG.info(
          "Soft delete functionVersions count: {} which versions are older than or equal to"
              + " versionRetentionLine: {}, the current functionId and version is: <{}, {}>.",
          deletedCount,
          versionRetentionLine,
          functionCurVersion.functionId(),
          functionCurVersion.version());
    }
    return totalDeletedCount;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getFunctionPOByIdentifier")
  FunctionPO getFunctionPOByIdentifier(NameIdentifier ident) {
    NameIdentifierUtil.checkFunction(ident);

    return functionPOFetcher().apply(ident);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateFunction")
  public <E extends Entity & HasIdentifier> FunctionEntity updateFunction(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    FunctionPO oldFunctionPO = getFunctionPOByIdentifier(identifier);
    FunctionEntity oldFunctionEntity = fromFunctionPO(oldFunctionPO, identifier.namespace());
    FunctionEntity newEntity = (FunctionEntity) updater.apply((E) oldFunctionEntity);
    Preconditions.checkArgument(
        Objects.equals(oldFunctionEntity.id(), newEntity.id()),
        "The updated function entity id: %s should be same with the entity id before: %s",
        newEntity.id(),
        oldFunctionEntity.id());

    try {
      FunctionPO newFunctionPO = updateFunctionPO(oldFunctionPO, newEntity);
      // Insert a new version and update function meta
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  FunctionVersionMetaMapper.class,
                  mapper -> mapper.insertFunctionVersionMeta(newFunctionPO.functionVersionPO())),
          () ->
              SessionUtils.doWithoutCommit(
                  FunctionMetaMapper.class,
                  mapper -> mapper.updateFunctionMeta(newFunctionPO, oldFunctionPO)));

      return newEntity;
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.FUNCTION, newEntity.nameIdentifier().toString());
      throw re;
    }
  }

  private Function<NameIdentifier, FunctionPO> functionPOFetcher() {
    return GravitinoEnv.getInstance().cacheEnabled()
        ? this::getFunctionPOBySchemaId
        : this::getFunctionPOByFullQualifiedName;
  }

  private FunctionPO getFunctionPOBySchemaId(NameIdentifier ident) {
    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(ident.namespace().levels()), Entity.EntityType.SCHEMA);

    FunctionPO functionPO =
        SessionUtils.getWithoutCommit(
            FunctionMetaMapper.class,
            mapper -> mapper.selectFunctionMetaBySchemaIdAndName(schemaId, ident.name()));

    if (functionPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.FUNCTION.name().toLowerCase(Locale.ROOT),
          ident.toString());
    }
    return functionPO;
  }

  private FunctionPO getFunctionPOByFullQualifiedName(NameIdentifier ident) {
    String[] namespaceLevels = ident.namespace().levels();
    FunctionPO functionPO =
        SessionUtils.getWithoutCommit(
            FunctionMetaMapper.class,
            mapper ->
                mapper.selectFunctionMetaByFullQualifiedName(
                    namespaceLevels[0], namespaceLevels[1], namespaceLevels[2], ident.name()));

    if (functionPO == null || functionPO.functionId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.FUNCTION.name().toLowerCase(Locale.ROOT),
          ident.name());
    }

    if (functionPO.schemaId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          namespaceLevels[2]);
    }
    return functionPO;
  }

  private List<FunctionPO> listFunctionPOs(Namespace namespace) {
    return functionListFetcher().apply(namespace);
  }

  private List<FunctionPO> listFunctionPOsBySchemaId(Namespace namespace) {
    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(namespace.levels()), Entity.EntityType.SCHEMA);
    return SessionUtils.getWithoutCommit(
        FunctionMetaMapper.class, mapper -> mapper.listFunctionPOsBySchemaId(schemaId));
  }

  private Function<Namespace, List<FunctionPO>> functionListFetcher() {
    return GravitinoEnv.getInstance().cacheEnabled()
        ? this::listFunctionPOsBySchemaId
        : this::listFunctionPOsByFullQualifiedName;
  }

  private List<FunctionPO> listFunctionPOsByFullQualifiedName(Namespace namespace) {
    String[] namespaceLevels = namespace.levels();
    List<FunctionPO> functionPOs =
        SessionUtils.getWithoutCommit(
            FunctionMetaMapper.class,
            mapper ->
                mapper.listFunctionPOsByFullQualifiedName(
                    namespaceLevels[0], namespaceLevels[1], namespaceLevels[2]));
    if (functionPOs.isEmpty() || functionPOs.get(0).schemaId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          namespaceLevels[2]);
    }
    return functionPOs.stream().filter(po -> po.functionId() != null).collect(Collectors.toList());
  }

  private void fillFunctionPOBuilderParentEntityId(
      FunctionPO.FunctionPOBuilder builder, Namespace ns) {
    NamespaceUtil.checkFunction(ns);
    NamespacedEntityId namespacedEntityId =
        EntityIdService.getEntityIds(NameIdentifier.of(ns.levels()), Entity.EntityType.SCHEMA);
    builder.withMetalakeId(namespacedEntityId.namespaceIds()[0]);
    builder.withCatalogId(namespacedEntityId.namespaceIds()[1]);
    builder.withSchemaId(namespacedEntityId.entityId());
  }

  private FunctionPO updateFunctionPO(FunctionPO oldFunctionPO, FunctionEntity newFunction) {
    Integer newVersion = oldFunctionPO.functionLatestVersion() + 1;
    FunctionPO.FunctionPOBuilder builder =
        FunctionPO.builder()
            .withMetalakeId(oldFunctionPO.metalakeId())
            .withCatalogId(oldFunctionPO.catalogId())
            .withSchemaId(oldFunctionPO.schemaId())
            .withFunctionLatestVersion(newVersion)
            .withFunctionCurrentVersion(newVersion);
    return buildFunctionPO(newFunction, builder, newVersion);
  }
}
