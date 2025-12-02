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
import java.util.Objects;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.helper.CatalogIds;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetVersionMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionAliasRelMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/**
 * The service class for catalog metadata. It provides the basic database operations for catalog.
 */
public class CatalogMetaService {
  private static final CatalogMetaService INSTANCE = new CatalogMetaService();

  public static CatalogMetaService getInstance() {
    return INSTANCE;
  }

  private CatalogMetaService() {}

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getCatalogPOByName")
  public CatalogPO getCatalogPOByName(String metalakeName, String catalogName) {
    CatalogPO catalogPO =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogMetaByName(metalakeName, catalogName));

    if (catalogPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          catalogName);
    }
    return catalogPO;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getCatalogIdByMetalakeAndCatalogName")
  public CatalogIds getCatalogIdByMetalakeAndCatalogName(String metalakeName, String catalogName) {
    CatalogIds catalogIds =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper ->
                mapper.selectCatalogIdByMetalakeNameAndCatalogName(metalakeName, catalogName));
    if (catalogIds == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          catalogName);
    }
    return catalogIds;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getCatalogIdByMetalakeIdAndName")
  public Long getCatalogIdByMetalakeIdAndName(Long metalakeId, String catalogName) {
    Long catalogId =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogIdByMetalakeIdAndName(metalakeId, catalogName));

    if (catalogId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          catalogName);
    }
    return catalogId;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getCatalogIdByName")
  public Long getCatalogIdByName(String metalakeName, String catalogName) {
    Long catalogId =
        SessionUtils.doWithCommitAndFetchResult(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogIdByName(metalakeName, catalogName));

    if (catalogId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          catalogName);
    }
    return catalogId;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getCatalogByIdentifier")
  public CatalogEntity getCatalogByIdentifier(NameIdentifier identifier) {
    NameIdentifierUtil.checkCatalog(identifier);
    String catalogName = identifier.name();

    CatalogPO catalogPO = getCatalogPOByName(identifier.namespace().level(0), catalogName);

    return POConverters.fromCatalogPO(catalogPO, identifier.namespace());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listCatalogsByNamespace")
  public List<CatalogEntity> listCatalogsByNamespace(Namespace namespace) {
    NamespaceUtil.checkCatalog(namespace);
    List<CatalogPO> catalogPOS =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.listCatalogPOsByMetalakeName(namespace.level(0)));

    return POConverters.fromCatalogPOs(catalogPOS, namespace);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertCatalog")
  public void insertCatalog(CatalogEntity catalogEntity, boolean overwrite) throws IOException {
    try {
      NameIdentifierUtil.checkCatalog(catalogEntity.nameIdentifier());

      String metalake = NameIdentifierUtil.getMetalake(catalogEntity.nameIdentifier());
      Long metalakeId =
          EntityIdService.getEntityId(NameIdentifier.of(metalake), Entity.EntityType.METALAKE);

      SessionUtils.doWithCommit(
          CatalogMetaMapper.class,
          mapper -> {
            CatalogPO po = POConverters.initializeCatalogPOWithVersion(catalogEntity, metalakeId);
            if (overwrite) {
              mapper.insertCatalogMetaOnDuplicateKeyUpdate(po);
            } else {
              mapper.insertCatalogMeta(po);
            }
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.CATALOG, catalogEntity.nameIdentifier().toString());
      throw re;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateCatalog")
  public <E extends Entity & HasIdentifier> CatalogEntity updateCatalog(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    NameIdentifierUtil.checkCatalog(identifier);

    String catalogName = identifier.name();

    CatalogPO oldCatalogPO = getCatalogPOByName(identifier.namespace().level(0), catalogName);

    CatalogEntity oldCatalogEntity =
        POConverters.fromCatalogPO(oldCatalogPO, identifier.namespace());
    CatalogEntity newEntity = (CatalogEntity) updater.apply((E) oldCatalogEntity);
    Preconditions.checkArgument(
        Objects.equals(oldCatalogEntity.id(), newEntity.id()),
        "The updated catalog entity id: %s should be same with the catalog entity id before: %s",
        newEntity.id(),
        oldCatalogEntity.id());

    Integer updateResult;
    try {
      updateResult =
          SessionUtils.doWithCommitAndFetchResult(
              CatalogMetaMapper.class,
              mapper ->
                  mapper.updateCatalogMeta(
                      POConverters.updateCatalogPOWithVersion(
                          oldCatalogPO, newEntity, oldCatalogPO.getMetalakeId()),
                      oldCatalogPO));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.CATALOG, newEntity.nameIdentifier().toString());
      throw re;
    }

    if (updateResult > 0) {
      return newEntity;
    } else {
      throw new IOException("Failed to update the entity: " + identifier);
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteCatalog")
  public boolean deleteCatalog(NameIdentifier identifier, boolean cascade) {
    NameIdentifierUtil.checkCatalog(identifier);

    String catalogName = identifier.name();
    long catalogId = EntityIdService.getEntityId(identifier, Entity.EntityType.CATALOG);

    if (cascade) {
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  CatalogMetaMapper.class,
                  mapper -> mapper.softDeleteCatalogMetasByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  SchemaMetaMapper.class,
                  mapper -> mapper.softDeleteSchemaMetasByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  TableMetaMapper.class,
                  mapper -> mapper.softDeleteTableMetasByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  TableColumnMapper.class,
                  mapper -> mapper.softDeleteColumnsByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  FilesetMetaMapper.class,
                  mapper -> mapper.softDeleteFilesetMetasByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  FilesetVersionMapper.class,
                  mapper -> mapper.softDeleteFilesetVersionsByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  TopicMetaMapper.class,
                  mapper -> mapper.softDeleteTopicMetasByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  OwnerMetaMapper.class, mapper -> mapper.softDeleteOwnerRelByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  SecurableObjectMapper.class,
                  mapper -> mapper.softDeleteObjectRelsByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  TagMetadataObjectRelMapper.class,
                  mapper -> mapper.softDeleteTagMetadataObjectRelsByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  PolicyMetadataObjectRelMapper.class,
                  mapper -> mapper.softDeletePolicyMetadataObjectRelsByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  ModelVersionAliasRelMapper.class,
                  mapper -> mapper.softDeleteModelVersionAliasRelsByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  ModelVersionMetaMapper.class,
                  mapper -> mapper.softDeleteModelVersionMetasByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  ModelMetaMapper.class,
                  mapper -> mapper.softDeleteModelMetasByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  StatisticMetaMapper.class,
                  mapper -> mapper.softDeleteStatisticsByCatalogId(catalogId)));
    } else {
      List<SchemaEntity> schemaEntities =
          SchemaMetaService.getInstance()
              .listSchemasByNamespace(
                  NamespaceUtil.ofSchema(identifier.namespace().level(0), catalogName));
      if (!schemaEntities.isEmpty()) {
        throw new NonEmptyEntityException(
            "Entity %s has sub-entities, you should remove sub-entities first", identifier);
      }
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  CatalogMetaMapper.class,
                  mapper -> mapper.softDeleteCatalogMetasByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  OwnerMetaMapper.class,
                  mapper ->
                      mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                          catalogId, MetadataObject.Type.CATALOG.name())),
          () ->
              SessionUtils.doWithoutCommit(
                  SecurableObjectMapper.class,
                  mapper ->
                      mapper.softDeleteObjectRelsByMetadataObject(
                          catalogId, MetadataObject.Type.CATALOG.name())),
          () ->
              SessionUtils.doWithoutCommit(
                  TagMetadataObjectRelMapper.class,
                  mapper ->
                      mapper.softDeleteTagMetadataObjectRelsByMetadataObject(
                          catalogId, MetadataObject.Type.CATALOG.name())),
          () ->
              SessionUtils.doWithoutCommit(
                  StatisticMetaMapper.class,
                  mapper -> mapper.softDeleteStatisticsByEntityId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  PolicyMetadataObjectRelMapper.class,
                  mapper ->
                      mapper.softDeletePolicyMetadataObjectRelsByMetadataObject(
                          catalogId, MetadataObject.Type.CATALOG.name())));
    }

    return true;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteCatalogMetasByLegacyTimeline")
  public int deleteCatalogMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        CatalogMetaMapper.class,
        mapper -> {
          return mapper.deleteCatalogMetasByLegacyTimeline(legacyTimeline, limit);
        });
  }
}
