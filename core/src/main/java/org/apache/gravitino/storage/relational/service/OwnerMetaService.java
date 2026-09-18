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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.GroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.po.GroupOwnerRelPO;
import org.apache.gravitino.storage.relational.po.GroupPO;
import org.apache.gravitino.storage.relational.po.OwnerRelForDeletion;
import org.apache.gravitino.storage.relational.po.OwnerRelPO;
import org.apache.gravitino.storage.relational.po.UserOwnerRelPO;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class is an utilization class to retrieve owner relation. */
public class OwnerMetaService {

  private static final Logger LOG = LoggerFactory.getLogger(OwnerMetaService.class);

  private static final OwnerMetaService INSTANCE = new OwnerMetaService();

  private OwnerMetaService() {}

  public static OwnerMetaService getInstance() {
    return INSTANCE;
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "getOwner")
  public Optional<Entity> getOwner(NameIdentifier identifier, Entity.EntityType type) {

    Long entityId = EntityIdService.getEntityId(identifier, type);

    UserPO userPO =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class,
            mapper -> mapper.selectUserOwnerMetaByMetadataObjectIdAndType(entityId, type.name()));

    if (userPO != null) {
      return Optional.of(
          POConverters.fromUserPO(
              userPO,
              Collections.emptyList(),
              AuthorizationUtils.ofUserNamespace(NameIdentifierUtil.getMetalake(identifier))));
    }

    GroupPO groupPO =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class,
            mapper -> mapper.selectGroupOwnerMetaByMetadataObjectIdAndType(entityId, type.name()));

    if (groupPO != null) {
      return Optional.of(
          POConverters.fromGroupPO(
              groupPO,
              Collections.emptyList(),
              AuthorizationUtils.ofGroupNamespace(NameIdentifierUtil.getMetalake(identifier))));
    }

    return Optional.empty();
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchGetOwner")
  public List<RelationalEntity<?>> batchGetOwner(
      List<NameIdentifier> identifiers, Entity.EntityType type) {
    if (CollectionUtils.isEmpty(identifiers)) {
      return new ArrayList<>();
    }
    String metalake = NameIdentifierUtil.getMetalake(identifiers.get(0));
    for (NameIdentifier identifier : identifiers) {
      Preconditions.checkArgument(
          Objects.equals(NameIdentifierUtil.getMetalake(identifier), metalake),
          "identifiers should in one metalake");
    }
    List<RelationalEntity<?>> result = new ArrayList<>();
    Map<Long, NameIdentifier> nameIdentifierMap = new HashMap<>();
    List<Long> entityIds =
        identifiers.stream()
            .map(
                identifier -> {
                  long entityId = EntityIdService.getEntityId(identifier, type);
                  nameIdentifierMap.put(entityId, identifier);
                  return entityId;
                })
            .collect(Collectors.toList());

    // Get user owners
    List<UserOwnerRelPO> userPOList =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class,
            mapper ->
                mapper.batchSelectUserOwnerMetaByMetadataObjectIdAndType(entityIds, type.name()));
    if (CollectionUtils.isNotEmpty(userPOList)) {
      userPOList.forEach(
          userPO -> {
            UserEntity userEntity =
                POConverters.fromUserPO(
                    userPO, Collections.emptyList(), AuthorizationUtils.ofUserNamespace(metalake));
            result.add(
                new RelationalEntity<>(
                    SupportsRelationOperations.Type.OWNER_REL,
                    nameIdentifierMap.get(userPO.getMetadataObjectId()),
                    type,
                    userEntity));
          });
    }

    // Get group owners
    List<GroupOwnerRelPO> groupPOList =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class,
            mapper ->
                mapper.batchSelectGroupOwnerMetaByMetadataObjectIdAndType(entityIds, type.name()));
    if (CollectionUtils.isNotEmpty(groupPOList)) {
      groupPOList.forEach(
          groupPO -> {
            GroupEntity groupEntity =
                POConverters.fromGroupPO(
                    groupPO,
                    Collections.emptyList(),
                    AuthorizationUtils.ofGroupNamespace(metalake));
            result.add(
                new RelationalEntity<>(
                    SupportsRelationOperations.Type.OWNER_REL,
                    nameIdentifierMap.get(groupPO.getMetadataObjectId()),
                    type,
                    groupEntity));
          });
    }

    return result;
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "setOwner")
  public void setOwner(
      NameIdentifier entity,
      Entity.EntityType entityType,
      NameIdentifier owner,
      Entity.EntityType ownerType) {
    String metalake = NameIdentifierUtil.getMetalake(entity);
    long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);

    Long entityId = EntityIdService.getEntityId(entity, entityType);
    Long ownerId = EntityIdService.getEntityId(owner, ownerType);

    OwnerRelPO ownerRelPO =
        POConverters.initializeOwnerRelPOsWithVersion(
            metalakeId, ownerType.name(), ownerId, entityType.name(), entityId);
    String metadataObjectType =
        NameIdentifierUtil.toMetadataObject(entity, entityType).type().name();
    assignOwner(
        metalake,
        metalakeId,
        owner,
        ownerType,
        ownerId,
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper ->
                    mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                        entityId, metadataObjectType)),
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class, mapper -> mapper.insertOwnerRel(ownerRelPO)));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchSetOwner")
  public void batchSetOwners(
      List<NameIdentifier> ownedObjects,
      Entity.EntityType ownedObjectType,
      NameIdentifier ownerIdent,
      Entity.EntityType ownerType) {
    if (CollectionUtils.isEmpty(ownedObjects)) {
      return;
    }

    String metalake = NameIdentifierUtil.getMetalake(ownedObjects.get(0));
    for (NameIdentifier entity : ownedObjects) {
      Preconditions.checkArgument(
          Objects.equals(NameIdentifierUtil.getMetalake(entity), metalake),
          "All owned objects must be in the same metalake");
    }

    long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
    Long ownerId = EntityIdService.getEntityId(ownerIdent, ownerType);

    // Resolve every object first and write in stable id order, so two batches that overlap take
    // the same row locks in the same order and cannot deadlock each other.
    List<Long> entityIds = new ArrayList<>(ownedObjects.size());
    for (NameIdentifier entity : ownedObjects) {
      entityIds.add(EntityIdService.getEntityId(entity, ownedObjectType));
    }
    entityIds.sort(Comparator.naturalOrder());
    String metadataObjectType =
        NameIdentifierUtil.toMetadataObject(ownedObjects.get(0), ownedObjectType).type().name();

    List<OwnerRelForDeletion> deletions = new ArrayList<>(entityIds.size());
    List<OwnerRelPO> ownerRelPOs = new ArrayList<>(entityIds.size());
    for (Long entityId : entityIds) {
      deletions.add(new OwnerRelForDeletion(entityId, metadataObjectType));
      ownerRelPOs.add(
          POConverters.initializeOwnerRelPOsWithVersion(
              metalakeId, ownerType.name(), ownerId, ownedObjectType.name(), entityId));
    }

    assignOwner(
        metalake,
        metalakeId,
        ownerIdent,
        ownerType,
        ownerId,
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper -> mapper.batchSoftDeleteOwnerRelByMetadataObjects(deletions)),
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class, mapper -> mapper.batchInsertOwnerRels(ownerRelPOs)));
  }

  /**
   * Runs one owner assignment as a single transaction: fence the metalake and the owner principal
   * on the identity the caller observed, retire the previous owner rows, then insert the new ones.
   *
   * <p>The metalake is held first because its cascade retires owner rows; the principal is held
   * next because its deletion does the same. Both are shared locks, so concurrent assignments only
   * wait for an in-flight deletion, not for each other. A principal that turns out deleted,
   * replaced under the same name, or moved to another metalake fails as not found instead of
   * leaving a live row that points nowhere.
   *
   * <p>The previous owner rows are retired by a soft-delete keyed on the object, and {@code
   * uk_mi_mo_del} allows one live row per object. Two assignments that both start when the object
   * has no live row cannot see each other's insert: the second one fails the unique key once the
   * first commits, and is then replayed once so that it retires the row it could not see. That
   * makes the outcome "last assignment wins" without a lock on the object itself.
   */
  private void assignOwner(
      String metalake,
      long metalakeId,
      NameIdentifier owner,
      Entity.EntityType ownerType,
      long ownerId,
      Runnable retirePreviousOwners,
      Runnable insertOwners) {
    for (int attempt = 0; ; attempt++) {
      try {
        SessionUtils.doMultipleWithCommit(
            () -> lockMetalakeForOwnerWrite(metalake, metalakeId),
            () -> lockPrincipalForOwnerWrite(owner, ownerType, ownerId, metalakeId),
            retirePreviousOwners,
            insertOwners);
        return;
      } catch (RuntimeException e) {
        if (attempt > 0 || !isDuplicateOwnerRow(e)) {
          throw e;
        }
        LOG.debug("Owner assignment lost a race on {} and is replayed once", owner, e);
      }
    }
  }

  private void lockMetalakeForOwnerWrite(String metalake, long metalakeId) {
    OccWriteSupport.lockParentForChildWrite(
        metalake,
        Entity.EntityType.METALAKE,
        () ->
            SessionUtils.getWithoutCommit(
                MetalakeMetaMapper.class,
                mapper -> mapper.selectMetalakeMetaByIdForShare(metalakeId)),
        null,
        current -> Objects.equals(current.getMetalakeName(), metalake));
  }

  private void lockPrincipalForOwnerWrite(
      NameIdentifier owner, Entity.EntityType ownerType, long ownerId, long metalakeId) {
    switch (ownerType) {
      case USER:
        OccWriteSupport.lockParentForChildWrite(
            owner.name(),
            ownerType,
            () ->
                SessionUtils.getWithoutCommit(
                    UserMetaMapper.class, mapper -> mapper.selectUserMetaByIdForShare(ownerId)),
            null,
            current ->
                Objects.equals(current.getMetalakeId(), metalakeId)
                    && Objects.equals(current.getUserName(), owner.name()));
        return;
      case GROUP:
        OccWriteSupport.lockParentForChildWrite(
            owner.name(),
            ownerType,
            () ->
                SessionUtils.getWithoutCommit(
                    GroupMetaMapper.class, mapper -> mapper.selectGroupMetaByIdForShare(ownerId)),
            null,
            current ->
                Objects.equals(current.getMetalakeId(), metalakeId)
                    && Objects.equals(current.getGroupName(), owner.name()));
        return;
      default:
        throw new IllegalArgumentException("Unsupported owner type: " + ownerType);
    }
  }

  /** Whether the failure is the unique-key violation raised by a second live owner row. */
  private static boolean isDuplicateOwnerRow(RuntimeException e) {
    try {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.USER, "owner");
      return false;
    } catch (EntityAlreadyExistsException duplicate) {
      return true;
    } catch (IOException other) {
      return false;
    }
  }
}
