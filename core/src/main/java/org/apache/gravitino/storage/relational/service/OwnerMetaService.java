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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationEdgeTarget;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.exceptions.NoSuchEntityException;
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
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** This class is an utilization class to retrieve owner relation. */
public class OwnerMetaService {

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
        NameIdentifierUtil.toMetadataObjectForRead(entity, entityType).type().name();
    assignOwner(
        metalake,
        metalakeId,
        owner,
        ownerType,
        ownerId,
        List.of(entityId),
        entityType,
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

  /**
   * Initializes a created table's owner without resolving the table by name.
   *
   * <p>The metalake, exact table ID and principal are fenced in the same order as ordinary owner
   * assignment. While the table lock serializes owner writers, a fresh read at READ_COMMITTED
   * bypasses earlier session-cached owner results. Clearing the query cache does not release the
   * database locks. Skips perform no relation write.
   *
   * @param update a table OWNER_REL initialization with an expected source ID and one user target
   * @return the assigned user, or an empty list if the table was deleted or already has an owner
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "initializeOwner")
  public List<UserEntity> initializeOwner(RelationUpdate update) {
    Preconditions.checkArgument(
        update.sourceEntityType() == Entity.EntityType.TABLE,
        "Initial owner assignment currently supports tables only");
    Preconditions.checkArgument(
        update.expectedSourceId().isPresent(),
        "Initial owner assignment requires the created entity ID");
    RelationEdgeTarget[] targets = update.targetsToAdd();
    Preconditions.checkArgument(
        targets.length == 1
            && targets[0].entityType() == Entity.EntityType.USER
            && update.targetsToRemove().length == 0
            && !update.hasRelationValues()
            && !update.auditInfo().isPresent(),
        "Initial owner assignment requires one user, no removals, values or explicit audit metadata");
    long entityId = update.expectedSourceId().get();
    NameIdentifier owner = targets[0].nameIdentifier();
    String metalake = NameIdentifierUtil.getMetalake(update.sourceIdentifier());
    Preconditions.checkArgument(
        Objects.equals(metalake, NameIdentifierUtil.getMetalake(owner)),
        "The table and initial owner must be in the same metalake");
    final long metalakeId;
    try {
      metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
    } catch (NoSuchEntityException missing) {
      return Collections.emptyList();
    }
    AtomicReference<UserEntity> assigned = new AtomicReference<>();
    SessionUtils.doMultipleWithCommit(
        () -> {
          try {
            lockMetalakeForOwnerWrite(metalake, metalakeId, false);
          } catch (NoSuchEntityException missing) {
            return;
          }
          Long liveId =
              SessionUtils.getWithoutCommit(
                  OwnerMetaMapper.class,
                  mapper ->
                      mapper.selectMetadataObjectIdForUpdate(
                          entityId, metalakeId, Entity.EntityType.TABLE));
          if (liveId == null) {
            return;
          }
          long ownerId = EntityIdService.getEntityId(owner, Entity.EntityType.USER);
          UserPO user = lockUserForOwnerWrite(owner, ownerId, metalakeId);
          if (SessionUtils.getWithoutCommitFresh(
                  OwnerMetaMapper.class,
                  mapper -> mapper.selectOwnerByMetadataObjectIdAndType(entityId, "TABLE"))
              != null) {
            return;
          }
          OwnerRelPO relation =
              POConverters.initializeOwnerRelPOsWithVersion(
                  metalakeId,
                  Entity.EntityType.USER.name(),
                  ownerId,
                  Entity.EntityType.TABLE.name(),
                  entityId);
          SessionUtils.doWithoutCommit(
              OwnerMetaMapper.class, mapper -> mapper.insertOwnerRel(relation));
          assigned.set(
              POConverters.fromUserPO(
                  user, Collections.emptyList(), AuthorizationUtils.ofUserNamespace(metalake)));
        });
    return assigned.get() == null ? Collections.emptyList() : List.of(assigned.get());
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
        NameIdentifierUtil.toMetadataObjectForRead(ownedObjects.get(0), ownedObjectType)
            .type()
            .name();

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
        entityIds,
        ownedObjectType,
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper -> mapper.batchSoftDeleteOwnerRelByMetadataObjects(deletions)),
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class, mapper -> mapper.batchInsertOwnerRels(ownerRelPOs)));
  }

  /**
   * Serializes assignments on the owned object's row, including the first assignment when no owner
   * relation exists. The metalake is locked first to fence cascade deletion; object rows are locked
   * in stable ID order; the principal is then fenced before owner relations change.
   */
  private void assignOwner(
      String metalake,
      long metalakeId,
      NameIdentifier owner,
      Entity.EntityType ownerType,
      long ownerId,
      List<Long> entityIds,
      Entity.EntityType ownedObjectType,
      Runnable retirePreviousOwners,
      Runnable insertOwners) {
    SessionUtils.doMultipleWithCommit(
        () ->
            lockMetalakeForOwnerWrite(
                metalake, metalakeId, ownedObjectType == Entity.EntityType.METALAKE),
        () -> lockOwnedObjectsForOwnerWrite(entityIds, ownedObjectType, metalakeId),
        () -> lockPrincipalForOwnerWrite(owner, ownerType, ownerId, metalakeId),
        retirePreviousOwners,
        insertOwners);
  }

  private void lockMetalakeForOwnerWrite(String metalake, long metalakeId, boolean exclusive) {
    OccWriteSupport.lockParentForChildWrite(
        metalake,
        Entity.EntityType.METALAKE,
        () ->
            SessionUtils.getWithoutCommit(
                MetalakeMetaMapper.class,
                mapper ->
                    exclusive
                        ? mapper.selectMetalakeMetaByIdForUpdate(metalakeId)
                        : mapper.selectMetalakeMetaByIdForShare(metalakeId)),
        null,
        current -> Objects.equals(current.getMetalakeName(), metalake));
  }

  private void lockOwnedObjectsForOwnerWrite(
      List<Long> entityIds, Entity.EntityType entityType, long metalakeId) {
    if (entityType == Entity.EntityType.METALAKE) {
      return;
    }
    for (Long entityId : entityIds) {
      OccWriteSupport.lockParentForChildWrite(
          String.valueOf(entityId),
          entityType,
          () ->
              SessionUtils.getWithoutCommit(
                  OwnerMetaMapper.class,
                  mapper ->
                      mapper.selectMetadataObjectIdForUpdate(entityId, metalakeId, entityType)),
          null,
          current -> Objects.equals(current, entityId));
    }
  }

  private void lockPrincipalForOwnerWrite(
      NameIdentifier owner, Entity.EntityType ownerType, long ownerId, long metalakeId) {
    switch (ownerType) {
      case USER:
        lockUserForOwnerWrite(owner, ownerId, metalakeId);
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

  private UserPO lockUserForOwnerWrite(NameIdentifier owner, long ownerId, long metalakeId) {
    return OccWriteSupport.lockParentForChildWrite(
        owner.name(),
        Entity.EntityType.USER,
        () ->
            SessionUtils.getWithoutCommit(
                UserMetaMapper.class, mapper -> mapper.selectUserMetaByIdForShare(ownerId)),
        null,
        current ->
            Objects.equals(current.getMetalakeId(), metalakeId)
                && Objects.equals(current.getUserName(), owner.name()));
  }
}
