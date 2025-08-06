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
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetVersionMapper;
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
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/**
 * The service class for metalake metadata. It provides the basic database operations for metalake.
 */
public class MetalakeMetaService {
  private static final MetalakeMetaService INSTANCE = new MetalakeMetaService();

  public static MetalakeMetaService getInstance() {
    return INSTANCE;
  }

  private MetalakeMetaService() {}

  public List<BaseMetalake> listMetalakes() {
    List<MetalakePO> metalakePOS =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, MetalakeMetaMapper::listMetalakePOs);
    return POConverters.fromMetalakePOs(metalakePOS);
  }

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

  // Metalake may be deleted, so the MetalakePO may be null.
  public MetalakePO getMetalakePOById(Long id) {
    MetalakePO metalakePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaById(id));
    return metalakePO;
  }

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
    Integer updateResult;
    try {
      updateResult =
          SessionUtils.doWithCommitAndFetchResult(
              MetalakeMetaMapper.class,
              mapper -> mapper.updateMetalakeMeta(newMetalakePO, oldMetalakePO));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.METALAKE, newMetalakeEntity.nameIdentifier().toString());
      throw re;
    }

    if (updateResult > 0) {
      return newMetalakeEntity;
    } else {
      throw new IOException("Failed to update the entity: " + ident);
    }
  }

  public boolean deleteMetalake(NameIdentifier ident, boolean cascade) {
    NameIdentifierUtil.checkMetalake(ident);
    Long metalakeId = getMetalakeIdByName(ident.name());
    if (metalakeId != null) {
      if (cascade) {
        SessionUtils.doMultipleWithCommit(
            () ->
                SessionUtils.doWithoutCommit(
                    MetalakeMetaMapper.class,
                    mapper -> mapper.softDeleteMetalakeMetaByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    CatalogMetaMapper.class,
                    mapper -> mapper.softDeleteCatalogMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    SchemaMetaMapper.class,
                    mapper -> mapper.softDeleteSchemaMetasByMetalakeId(metalakeId)),
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
                    mapper -> mapper.softDeleteJobMetasByMetalakeId(metalakeId)));
      } else {
        List<CatalogEntity> catalogEntities =
            CatalogMetaService.getInstance()
                .listCatalogsByNamespace(NamespaceUtil.ofCatalog(ident.name()));
        if (!catalogEntities.isEmpty()) {
          throw new NonEmptyEntityException(
              "Entity %s has sub-entities, you should remove sub-entities first", ident);
        }
        SessionUtils.doMultipleWithCommit(
            () ->
                SessionUtils.doWithoutCommit(
                    MetalakeMetaMapper.class,
                    mapper -> mapper.softDeleteMetalakeMetaByMetalakeId(metalakeId)),
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

  public int deleteMetalakeMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    int[] metalakeDeleteCount = new int[] {0};
    int[] ownerRelDeleteCount = new int[] {0};
    SessionUtils.doMultipleWithCommit(
        () ->
            metalakeDeleteCount[0] =
                SessionUtils.doWithCommitAndFetchResult(
                    MetalakeMetaMapper.class,
                    mapper -> mapper.deleteMetalakeMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            ownerRelDeleteCount[0] =
                SessionUtils.doWithCommitAndFetchResult(
                    OwnerMetaMapper.class,
                    mapper -> mapper.deleteOwnerMetasByLegacyTimeline(legacyTimeline, limit)));
    return metalakeDeleteCount[0] + ownerRelDeleteCount[0];
  }
}
