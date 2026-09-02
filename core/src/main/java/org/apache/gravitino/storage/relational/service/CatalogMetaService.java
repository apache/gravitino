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
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.helper.CatalogIds;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetVersionMapper;
import org.apache.gravitino.storage.relational.mapper.FunctionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FunctionVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
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
import org.apache.gravitino.storage.relational.mapper.ViewMetaMapper;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
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

      String metalakeName = NameIdentifierUtil.getMetalake(catalogEntity.nameIdentifier());
      // This read runs before the transaction below, so it only tells us the metalake ID and name
      // we start from. The metalake may still be dropped or renamed right after it. That is why
      // lockMetalakeForCatalogCreate checks the row again inside the transaction.
      MetalakePO metalakePO =
          SessionUtils.getWithoutCommit(
              MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(metalakeName));
      if (metalakePO == null) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
            Entity.EntityType.METALAKE.name().toLowerCase(),
            metalakeName);
      }

      SessionUtils.doMultipleWithCommit(
          () -> lockMetalakeForCatalogCreate(metalakePO),
          () ->
              SessionUtils.doWithoutCommit(
                  CatalogMetaMapper.class,
                  mapper -> {
                    CatalogPO po =
                        POConverters.initializeCatalogPOWithVersion(
                            catalogEntity, metalakePO.getMetalakeId());
                    if (overwrite) {
                      mapper.insertCatalogMetaOnDuplicateKeyUpdate(po);
                    } else {
                      mapper.insertCatalogMeta(po);
                    }
                  }));
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

    try {
      SessionUtils.doMultipleWithCommit(
          () -> {
            // The UPDATE only matches the row if its version is still the one we read above, and
            // it writes the next version. So two servers that read the same catalog cannot both
            // apply their change: the second one updates no row.
            int updated =
                SessionUtils.getWithoutCommit(
                    CatalogMetaMapper.class,
                    mapper ->
                        mapper.updateCatalogMeta(
                            POConverters.updateCatalogPOWithVersion(
                                oldCatalogPO, newEntity, oldCatalogPO.getMetalakeId()),
                            oldCatalogPO));
            if (updated == 0) {
              // Zero rows can mean two different things: someone else changed the catalog, or the
              // catalog is gone. Let catalogWriteFailure tell them apart and pick the error.
              throw catalogWriteFailure(identifier, oldCatalogPO);
            }
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.CATALOG, newEntity.nameIdentifier().toString());
      throw re;
    }

    return newEntity;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteCatalog")
  public boolean deleteCatalog(NameIdentifier identifier, boolean cascade) {
    NameIdentifierUtil.checkCatalog(identifier);

    String catalogName = identifier.name();
    // Read the whole row, not just the ID, because the delete below needs the version we saw.
    CatalogPO catalogPO = getCatalogPOByName(identifier.namespace().level(0), catalogName);
    long catalogId = catalogPO.getCatalogId();

    if (cascade) {
      SessionUtils.doMultipleWithCommit(
          () -> {
            // Delete the parent first, then its children. The parent delete locks the catalog row,
            // and schema writes lock that same row before they touch a schema, so no schema can be
            // added or removed after this point. Anything that goes wrong later in this
            // transaction rolls this soft delete back with it.
            deleteCatalogWithVersion(identifier, catalogPO);
            deleteSchemasWithVersions(identifier, catalogId);
          },
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
                  FunctionMetaMapper.class,
                  mapper -> mapper.softDeleteFunctionMetasByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  FunctionVersionMetaMapper.class,
                  mapper -> mapper.softDeleteFunctionVersionMetasByCatalogId(catalogId)),
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
                  mapper -> mapper.softDeleteStatisticsByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  ViewMetaMapper.class,
                  mapper -> mapper.softDeleteViewMetasByCatalogId(catalogId)));
    } else {
      SessionUtils.doMultipleWithCommit(
          () -> {
            // Delete the catalog first and check for schemas afterwards. This order looks odd, but
            // it is what makes the check safe: the delete locks the catalog row, and schema
            // creation locks the same row before inserting. So a create either finishes before this
            // delete, in which case the check below sees its schema, or it waits until this
            // transaction ends. Checking first would leave a gap where a schema can be inserted
            // between the check and the delete. If the check does find a schema, the exception
            // rolls the soft delete back.
            deleteCatalogWithVersion(identifier, catalogPO);
            List<SchemaPO> schemaPOs =
                SessionUtils.getWithoutCommit(
                    SchemaMetaMapper.class, mapper -> mapper.listSchemaPOsByCatalogId(catalogId));
            if (!schemaPOs.isEmpty()) {
              throw new NonEmptyEntityException(
                  "Entity %s has sub-entities, you should remove sub-entities first", identifier);
            }
          },
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

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchGetCatalogByIdentifier")
  public List<CatalogEntity> batchGetCatalogByIdentifier(List<NameIdentifier> identifiers) {
    NameIdentifier firstIdent = identifiers.get(0);
    String metalakeName = firstIdent.namespace().level(0);
    List<String> catalogNames =
        identifiers.stream().map(NameIdentifier::name).collect(Collectors.toList());

    return SessionUtils.doWithCommitAndFetchResult(
        CatalogMetaMapper.class,
        mapper -> {
          List<CatalogPO> catalogPOs =
              mapper.batchSelectCatalogByIdentifier(metalakeName, catalogNames);
          return POConverters.fromCatalogPOs(catalogPOs, firstIdent.namespace());
        });
  }

  /**
   * Soft-deletes the catalog only if its version is still the one the caller read. A drop that
   * loses the race to another writer must not delete a catalog it never saw.
   */
  private void deleteCatalogWithVersion(NameIdentifier identifier, CatalogPO observedCatalogPO) {
    OccWriteSupport.deleteWithVersion(
        () ->
            SessionUtils.getWithoutCommit(
                CatalogMetaMapper.class,
                mapper ->
                    mapper.softDeleteCatalogMetasByCatalogId(
                        observedCatalogPO.getCatalogId(), observedCatalogPO.getCurrentVersion())),
        () -> catalogWriteFailure(identifier, observedCatalogPO));
  }

  /**
   * Holds the parent metalake row for the rest of the transaction, so the catalog cannot be created
   * below a metalake that is going away.
   *
   * <p>The lock is shared, not exclusive: many catalogs can be created under the same metalake at
   * the same time. Dropping a metalake takes an exclusive lock on this row, so a drop and a create
   * cannot overlap. Whoever gets the row first wins, and the loser either sees the metalake gone or
   * inserts under a metalake that is still there.
   *
   * <p>The name is compared again because the ID alone cannot tell a rename apart: the caller
   * looked the metalake up by name, so a renamed row means the name in the request no longer
   * exists.
   */
  private void lockMetalakeForCatalogCreate(MetalakePO observedMetalakePO) {
    OccWriteSupport.lockParentForChildWrite(
        observedMetalakePO.getMetalakeName(),
        Entity.EntityType.METALAKE,
        () ->
            SessionUtils.getWithoutCommit(
                MetalakeMetaMapper.class,
                mapper ->
                    mapper.selectMetalakeMetaByIdForShare(observedMetalakePO.getMetalakeId())),
        null,
        current -> Objects.equals(current.getMetalakeName(), observedMetalakePO.getMetalakeName()));
  }

  /**
   * Decides which error a failed compare-and-set should report. The write matched no row either
   * because someone else changed the catalog, which is a conflict, or because the catalog was
   * deleted or renamed away, which is a missing entity.
   */
  private RuntimeException catalogWriteFailure(
      NameIdentifier identifier, CatalogPO observedCatalogPO) {
    return OccWriteSupport.writeFailure(
        identifier,
        Entity.EntityType.CATALOG,
        () ->
            SessionUtils.getWithoutCommit(
                CatalogMetaMapper.class,
                mapper -> mapper.selectCatalogMetaByIdForUpdate(observedCatalogPO.getCatalogId())),
        null,
        current ->
            Objects.equals(current.getCatalogName(), observedCatalogPO.getCatalogName())
                && Objects.equals(current.getMetalakeId(), observedCatalogPO.getMetalakeId()));
  }

  /**
   * Soft-deletes every schema of the catalog, each one guarded by the version read here. The caller
   * must already hold the catalog row, so no schema can appear or disappear in between.
   */
  private void deleteSchemasWithVersions(NameIdentifier catalogIdentifier, Long catalogId) {
    List<SchemaPO> schemaPOs = listSchemaPOsForCascade(catalogId);
    OccWriteSupport.deleteChildrenWithVersions(
        catalogIdentifier,
        Entity.EntityType.SCHEMA,
        Entity.EntityType.CATALOG,
        schemaPOs,
        children ->
            SessionUtils.getWithoutCommit(
                SchemaMetaMapper.class,
                mapper -> mapper.softDeleteSchemaMetasWithVersion(children)));
  }

  /**
   * Reads the schemas that the cascade is about to delete. The caller already holds the catalog
   * row, so this snapshot cannot grow or shrink behind it. Kept separate so a test can pause the
   * cascade exactly here, between taking the lock and reading the children.
   */
  List<SchemaPO> listSchemaPOsForCascade(Long catalogId) {
    return SessionUtils.getWithoutCommit(
        SchemaMetaMapper.class, mapper -> mapper.listSchemaPOsByCatalogId(catalogId));
  }
}
