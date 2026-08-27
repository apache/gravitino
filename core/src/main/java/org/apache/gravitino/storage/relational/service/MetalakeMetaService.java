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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetVersionMapper;
import org.apache.gravitino.storage.relational.mapper.FunctionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FunctionVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.GroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.GroupRoleRelMapper;
import org.apache.gravitino.storage.relational.mapper.JobMetaMapper;
import org.apache.gravitino.storage.relational.mapper.JobTemplateMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionAliasRelMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyTagRelMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyVersionMapper;
import org.apache.gravitino.storage.relational.mapper.RoleMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper;
import org.apache.gravitino.storage.relational.mapper.ViewMetaMapper;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * The service class for metalake metadata. It provides the basic database operations for metalake.
 */
public class MetalakeMetaService {
  private static final MetalakeMetaService INSTANCE = new MetalakeMetaService();

  public static MetalakeMetaService getInstance() {
    return INSTANCE;
  }

  private MetalakeMetaService() {}

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listMetalakes")
  public List<BaseMetalake> listMetalakes() {
    List<MetalakePO> metalakePOS =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, MetalakeMetaMapper::listMetalakePOs);
    return POConverters.fromMetalakePOs(metalakePOS);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getMetalakeIdByName")
  public Long getMetalakeIdByName(String metalakeName) {
    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
    if (metalakeId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          metalakeName);
    }
    return metalakeId;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getMetalakeByIdentifier")
  public BaseMetalake getMetalakeByIdentifier(NameIdentifier ident) {
    NameIdentifierUtil.checkMetalake(ident);
    MetalakePO metalakePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(ident.name()));
    if (metalakePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          ident.toString());
    }
    return POConverters.fromMetalakePO(metalakePO);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertMetalake")
  public void insertMetalake(BaseMetalake baseMetalake, boolean overwrite) throws IOException {
    try {
      NameIdentifierUtil.checkMetalake(baseMetalake.nameIdentifier());
      SessionUtils.doWithCommit(
          MetalakeMetaMapper.class,
          mapper -> {
            MetalakePO po = POConverters.initializeMetalakePOWithVersion(baseMetalake);
            if (overwrite) {
              mapper.insertMetalakeMetaOnDuplicateKeyUpdate(po);
            } else {
              mapper.insertMetalakeMeta(po);
            }
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.METALAKE, baseMetalake.nameIdentifier().toString());
      throw re;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateMetalake")
  public <E extends Entity & HasIdentifier> BaseMetalake updateMetalake(
      NameIdentifier ident, Function<E, E> updater) throws IOException {
    NameIdentifierUtil.checkMetalake(ident);
    MetalakePO oldMetalakePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(ident.name()));
    if (oldMetalakePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          ident.toString());
    }

    BaseMetalake oldMetalakeEntity = POConverters.fromMetalakePO(oldMetalakePO);
    BaseMetalake newMetalakeEntity = (BaseMetalake) updater.apply((E) oldMetalakeEntity);
    Preconditions.checkArgument(
        Objects.equals(oldMetalakeEntity.id(), newMetalakeEntity.id()),
        "The updated metalake entity id: %s should be same with the metalake entity id before: %s",
        newMetalakeEntity.id(),
        oldMetalakeEntity.id());
    MetalakePO newMetalakePO =
        POConverters.updateMetalakePOWithVersion(oldMetalakePO, newMetalakeEntity);

    try {
      SessionUtils.doMultipleWithCommit(
          () -> {
            int updated =
                SessionUtils.getWithoutCommit(
                    MetalakeMetaMapper.class,
                    mapper -> mapper.updateMetalakeMeta(newMetalakePO, oldMetalakePO));
            if (updated == 0) {
              // The row may have a new version, or it may have been deleted or renamed. Re-read it
              // to return the correct conflict or missing-entity error.
              throw metalakeWriteFailure(
                  ident, oldMetalakePO.getMetalakeId(), oldMetalakePO.getMetalakeName());
            }
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.METALAKE, newMetalakeEntity.nameIdentifier().toString());
      throw re;
    }

    return newMetalakeEntity;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteMetalake")
  public boolean deleteMetalake(NameIdentifier ident, boolean cascade) {
    NameIdentifierUtil.checkMetalake(ident);
    MetalakePO metalakePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(ident.name()));
    if (metalakePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          ident.toString());
    }
    Long metalakeId = metalakePO.getMetalakeId();
    Long currentVersion = metalakePO.getCurrentVersion();
    if (metalakeId != null) {
      if (cascade) {
        SessionUtils.doMultipleWithCommit(
            () -> {
              // Take the parent lock before the child snapshot, so catalog creation cannot add a
              // child after the snapshot. A later failure rolls back this soft delete as well.
              deleteMetalakeWithVersion(ident, metalakeId, currentVersion);
              deleteCatalogsWithVersions(ident, metalakeId);
              deleteSchemasWithVersions(ident, listSchemaPOsForCascade(metalakeId));
            },
            () ->
                SessionUtils.doWithoutCommit(
                    TableMetaMapper.class,
                    mapper -> mapper.softDeleteTableMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    TableColumnMapper.class,
                    mapper -> mapper.softDeleteColumnsByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    FilesetMetaMapper.class,
                    mapper -> mapper.softDeleteFilesetMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    FilesetVersionMapper.class,
                    mapper -> mapper.softDeleteFilesetVersionsByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    TopicMetaMapper.class,
                    mapper -> mapper.softDeleteTopicMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    FunctionMetaMapper.class,
                    mapper -> mapper.softDeleteFunctionMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    FunctionVersionMetaMapper.class,
                    mapper -> mapper.softDeleteFunctionVersionMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    UserRoleRelMapper.class,
                    mapper -> mapper.softDeleteUserRoleRelByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    UserMetaMapper.class,
                    mapper -> mapper.softDeleteUserMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    GroupRoleRelMapper.class,
                    mapper -> mapper.softDeleteGroupRoleRelByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    GroupMetaMapper.class,
                    mapper -> mapper.softDeleteGroupMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    RoleMetaMapper.class,
                    mapper -> mapper.softDeleteRoleMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    SecurableObjectMapper.class,
                    mapper -> mapper.softDeleteSecurableObjectsByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    TagMetaMapper.class,
                    mapper -> mapper.softDeleteTagMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    TagMetadataObjectRelMapper.class,
                    mapper -> mapper.softDeleteTagMetadataObjectRelsByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    PolicyTagRelMapper.class, mapper -> mapper.softDeleteByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    PolicyMetaMapper.class,
                    mapper -> mapper.softDeletePolicyMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    PolicyVersionMapper.class,
                    mapper -> mapper.softDeletePolicyVersionsByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    OwnerMetaMapper.class,
                    mapper -> mapper.softDeleteOwnerRelByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    ModelVersionAliasRelMapper.class,
                    mapper -> mapper.softDeleteModelVersionAliasRelsByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    ModelVersionMetaMapper.class,
                    mapper -> mapper.softDeleteModelVersionMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    ModelMetaMapper.class,
                    mapper -> mapper.softDeleteModelMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    StatisticMetaMapper.class,
                    mapper -> mapper.softDeleteStatisticsByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    JobTemplateMetaMapper.class,
                    mapper -> mapper.softDeleteJobTemplateMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    JobMetaMapper.class,
                    mapper -> mapper.softDeleteJobMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    ViewMetaMapper.class,
                    mapper -> mapper.softDeleteViewMetasByMetalakeId(metalakeId)));
      } else {
        SessionUtils.doMultipleWithCommit(
            () -> {
              // Delete the metalake before checking its children. The UPDATE takes an exclusive
              // lock on the metalake row, and catalog creation locks the same row before inserting.
              // If creation gets its lock first, this delete waits and the check below sees the new
              // catalog. If this delete gets the lock first, creation waits until this transaction
              // commits or rolls back. Checking first would allow a catalog to be inserted between
              // the check and this delete. A non-empty result rolls back the soft delete.
              deleteMetalakeWithVersion(ident, metalakeId, currentVersion);
              List<CatalogPO> catalogPOs =
                  SessionUtils.getWithoutCommit(
                      CatalogMetaMapper.class,
                      mapper -> mapper.listCatalogPOsByMetalakeId(metalakeId));
              if (!catalogPOs.isEmpty()) {
                throw new NonEmptyEntityException(
                    "Entity %s has sub-entities, you should remove sub-entities first", ident);
              }
            },
            () ->
                SessionUtils.doWithoutCommit(
                    UserRoleRelMapper.class,
                    mapper -> mapper.softDeleteUserRoleRelByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    UserMetaMapper.class,
                    mapper -> mapper.softDeleteUserMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    GroupRoleRelMapper.class,
                    mapper -> mapper.softDeleteGroupRoleRelByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    GroupMetaMapper.class,
                    mapper -> mapper.softDeleteGroupMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    RoleMetaMapper.class,
                    mapper -> mapper.softDeleteRoleMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    SecurableObjectMapper.class,
                    mapper -> mapper.softDeleteSecurableObjectsByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    TagMetaMapper.class,
                    mapper -> mapper.softDeleteTagMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    TagMetadataObjectRelMapper.class,
                    mapper -> mapper.softDeleteTagMetadataObjectRelsByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    PolicyTagRelMapper.class, mapper -> mapper.softDeleteByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    OwnerMetaMapper.class,
                    mapper -> mapper.softDeleteOwnerRelByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    StatisticMetaMapper.class,
                    mapper -> mapper.softDeleteStatisticsByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    JobTemplateMetaMapper.class,
                    mapper -> mapper.softDeleteJobTemplateMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    JobMetaMapper.class,
                    mapper -> mapper.softDeleteJobMetasByMetalakeId(metalakeId)));
      }
    }
    return true;
  }

  void deleteMetalakeWithVersion(NameIdentifier identifier, Long metalakeId, Long currentVersion) {
    OccWriteSupport.deleteWithVersion(
        () ->
            SessionUtils.getWithoutCommit(
                MetalakeMetaMapper.class,
                mapper -> mapper.softDeleteMetalakeMetaByMetalakeId(metalakeId, currentVersion)),
        () -> metalakeWriteFailure(identifier, metalakeId, identifier.name()));
  }

  private RuntimeException metalakeWriteFailure(
      NameIdentifier identifier, Long metalakeId, String observedName) {
    return OccWriteSupport.writeFailure(
        identifier,
        Entity.EntityType.METALAKE,
        () ->
            SessionUtils.getWithoutCommit(
                MetalakeMetaMapper.class,
                mapper -> mapper.selectMetalakeMetaByIdForUpdate(metalakeId)),
        null,
        current -> Objects.equals(current.getMetalakeName(), observedName));
  }

  private void deleteCatalogsWithVersions(NameIdentifier metalakeIdentifier, Long metalakeId) {
    // Lock all catalog rows before taking the schema snapshot. Schema creation and deletion lock
    // their parent catalog, so they cannot add or remove a schema after this point. A schema alter
    // can still run, but the version check below detects it. Taking parent locks before child rows
    // also keeps the same lock order for every metalake cascade.
    List<CatalogPO> catalogPOs =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.listCatalogPOsByMetalakeIdForUpdate(metalakeId));
    OccWriteSupport.deleteChildrenWithVersions(
        metalakeIdentifier,
        Entity.EntityType.CATALOG,
        Entity.EntityType.METALAKE,
        catalogPOs,
        children ->
            SessionUtils.getWithoutCommit(
                CatalogMetaMapper.class,
                mapper -> mapper.softDeleteCatalogMetasWithVersion(children)));
  }

  List<SchemaPO> listSchemaPOsForCascade(Long metalakeId) {
    return SessionUtils.getWithoutCommit(
        SchemaMetaMapper.class, mapper -> mapper.listSchemaPOsByMetalakeId(metalakeId));
  }

  private void deleteSchemasWithVersions(
      NameIdentifier metalakeIdentifier, List<SchemaPO> schemaPOs) {
    OccWriteSupport.deleteChildrenWithVersions(
        metalakeIdentifier,
        Entity.EntityType.SCHEMA,
        Entity.EntityType.METALAKE,
        schemaPOs,
        children ->
            SessionUtils.getWithoutCommit(
                SchemaMetaMapper.class,
                mapper -> mapper.softDeleteSchemaMetasWithVersion(children)));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteMetalakeMetasByLegacyTimeline")
  public int deleteMetalakeMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    int[] metalakeDeleteCount = new int[] {0};
    int[] ownerRelDeleteCount = new int[] {0};
    SessionUtils.doMultipleWithCommit(
        () ->
            metalakeDeleteCount[0] =
                SessionUtils.getWithoutCommit(
                    MetalakeMetaMapper.class,
                    mapper -> mapper.deleteMetalakeMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            ownerRelDeleteCount[0] =
                SessionUtils.getWithoutCommit(
                    OwnerMetaMapper.class,
                    mapper -> mapper.deleteOwnerMetasByLegacyTimeline(legacyTimeline, limit)));
    return metalakeDeleteCount[0] + ownerRelDeleteCount[0];
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchGetMetalakeByIdentifier")
  public List<BaseMetalake> batchGetMetalakeByIdentifier(List<NameIdentifier> identifiers) {

    List<String> metalakeNames =
        identifiers.stream().map(NameIdentifier::name).collect(Collectors.toList());

    return SessionUtils.doWithCommitAndFetchResult(
        MetalakeMetaMapper.class,
        mapper -> {
          List<MetalakePO> metalakePOs = mapper.batchSelectMetalakeByName(metalakeNames);
          return POConverters.fromMetalakePOs(metalakePOs);
        });
  }
}
