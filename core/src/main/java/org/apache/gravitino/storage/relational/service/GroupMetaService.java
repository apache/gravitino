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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.PagedResult;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.GroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.GroupRoleRelMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.po.ExtendedGroupPO;
import org.apache.gravitino.storage.relational.po.GroupPO;
import org.apache.gravitino.storage.relational.po.GroupRoleRelPO;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** The service class for group metadata. It provides the basic database operations for group. */
public class GroupMetaService {
  private static final GroupMetaService INSTANCE = new GroupMetaService();

  public static GroupMetaService getInstance() {
    return INSTANCE;
  }

  private GroupMetaService() {}

  private GroupPO getGroupPOByMetalakeIdAndName(Long metalakeId, String groupName) {
    GroupPO GroupPO =
        SessionUtils.getWithoutCommit(
            GroupMetaMapper.class,
            mapper -> mapper.selectGroupMetaByMetalakeIdAndName(metalakeId, groupName));

    if (GroupPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.GROUP.name().toLowerCase(),
          groupName);
    }
    return GroupPO;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getGroupIdByMetalakeIdAndName")
  public Long getGroupIdByMetalakeIdAndName(Long metalakeId, String groupName) {
    Long groupId =
        SessionUtils.getWithoutCommit(
            GroupMetaMapper.class,
            mapper -> mapper.selectGroupIdBySchemaIdAndName(metalakeId, groupName));

    if (groupId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.GROUP.name().toLowerCase(),
          groupName);
    }
    return groupId;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getGroupByIdentifier")
  public GroupEntity getGroupByIdentifier(NameIdentifier identifier) {
    AuthorizationUtils.checkGroup(identifier);

    NameIdentifier metalakeIdent = NameIdentifier.of(NameIdentifierUtil.getMetalake(identifier));
    long metalakeId = EntityIdService.getEntityId(metalakeIdent, Entity.EntityType.METALAKE);
    GroupPO groupPO = getGroupPOByMetalakeIdAndName(metalakeId, identifier.name());
    List<RolePO> rolePOs = RoleMetaService.getInstance().listRolesByGroupId(groupPO.getGroupId());

    return POConverters.fromGroupPO(groupPO, rolePOs, identifier.namespace());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchGetGroupByIdentifier")
  public List<GroupEntity> batchGetGroupByIdentifier(List<NameIdentifier> identifiers) {
    if (identifiers == null || identifiers.isEmpty()) {
      return Collections.emptyList();
    }

    NameIdentifier firstIdent = identifiers.get(0);
    Namespace namespace = firstIdent.namespace();
    String metalake = NameIdentifierUtil.getMetalake(firstIdent);

    for (NameIdentifier identifier : identifiers) {
      AuthorizationUtils.checkGroup(identifier);
      Preconditions.checkArgument(
          identifier.namespace().equals(namespace),
          "All group identifiers must belong to the same namespace, expected %s but got %s",
          namespace,
          identifier.namespace());
    }

    long metalakeId =
        EntityIdService.getEntityId(NameIdentifier.of(metalake), Entity.EntityType.METALAKE);
    List<String> groupNames =
        identifiers.stream().map(NameIdentifier::name).collect(Collectors.toList());

    return SessionUtils.doWithCommitAndFetchResult(
        GroupMetaMapper.class,
        mapper -> {
          List<ExtendedGroupPO> extendedPOs =
              mapper.listExtendedGroupPOsByMetalakeIdAndNames(metalakeId, groupNames);
          return extendedPOs.stream()
              .map(po -> POConverters.fromExtendedGroupPO(po, namespace))
              .collect(Collectors.toList());
        });
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listGroupsByRoleIdent")
  public List<GroupEntity> listGroupsByRoleIdent(NameIdentifier roleIdent) {
    RoleEntity roleEntity = RoleMetaService.getInstance().getRoleByIdentifier(roleIdent);
    List<GroupPO> groupPOs =
        SessionUtils.getWithoutCommit(
            GroupMetaMapper.class, mapper -> mapper.listGroupsByRoleId(roleEntity.id()));
    return groupPOs.stream()
        .map(
            po ->
                POConverters.fromGroupPO(
                    po,
                    Collections.emptyList(),
                    AuthorizationUtils.ofGroupNamespace(roleIdent.namespace().level(0))))
        .collect(Collectors.toList());
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "insertGroup")
  public void insertGroup(GroupEntity groupEntity, boolean overwritten) throws IOException {
    try {
      AuthorizationUtils.checkGroup(groupEntity.nameIdentifier());

      NameIdentifier metalakeIdent =
          NameIdentifier.of(NameIdentifierUtil.getMetalake(groupEntity.nameIdentifier()));
      MetalakePO metalakePO =
          SessionUtils.getWithoutCommit(
              MetalakeMetaMapper.class,
              mapper -> mapper.selectMetalakeMetaByName(metalakeIdent.name()));
      if (metalakePO == null) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
            Entity.EntityType.METALAKE.name().toLowerCase(),
            metalakeIdent.name());
      }

      GroupPO.Builder builder = GroupPO.builder().withMetalakeId(metalakePO.getMetalakeId());
      GroupPO groupPO = POConverters.initializeGroupPOWithVersion(groupEntity, builder);

      List<Long> roleIds = Optional.ofNullable(groupEntity.roleIds()).orElse(Lists.newArrayList());
      List<GroupRoleRelPO> groupRoleRelPOS =
          POConverters.initializeGroupRoleRelsPOWithVersion(groupEntity, roleIds);

      SessionUtils.doMultipleWithCommit(
          () -> lockMetalakeForGroupCreate(metalakePO),
          () ->
              SessionUtils.doWithoutCommit(
                  GroupMetaMapper.class,
                  mapper -> {
                    if (overwritten) {
                      mapper.insertGroupMetaOnDuplicateKeyUpdate(groupPO);
                    } else {
                      mapper.insertGroupMeta(groupPO);
                    }
                  }),
          () -> {
            SessionUtils.doWithoutCommit(
                GroupRoleRelMapper.class,
                mapper -> {
                  if (overwritten) {
                    mapper.softDeleteGroupRoleRelByGroupId(groupEntity.id());
                  }
                  if (!groupRoleRelPOS.isEmpty()) {
                    mapper.batchInsertGroupRoleRel(groupRoleRelPOS);
                  }
                });
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.GROUP, groupEntity.nameIdentifier().toString());
      throw re;
    }
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "deleteGroup")
  public boolean deleteGroup(NameIdentifier identifier) {
    AuthorizationUtils.checkGroup(identifier);

    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
    GroupPO groupPO = getGroupPOByMetalakeIdAndName(metalakeId, identifier.name());
    deleteGroupWithVersion(identifier, groupPO);
    return true;
  }

  /**
   * Deletes the group whose version matches {@code observedGroupPO}, together with its role and
   * owner relations. Package-private so tests can hand in a deliberately stale PO; callers outside
   * this class go through {@link #deleteGroup(NameIdentifier)}, which reads the row first.
   *
   * @param identifier the group being deleted, used only to build the error
   * @param observedGroupPO the group row the caller observed, carrying the version to match
   */
  void deleteGroupWithVersion(NameIdentifier identifier, GroupPO observedGroupPO) {
    Long groupId = observedGroupPO.getGroupId();
    SessionUtils.doMultipleWithCommit(
        () -> {
          int deleted =
              SessionUtils.getWithoutCommit(
                  GroupMetaMapper.class,
                  mapper ->
                      mapper.softDeleteGroupMetaByGroupId(
                          groupId, observedGroupPO.getCurrentVersion()));
          if (deleted == 0) {
            throw groupWriteFailure(identifier, observedGroupPO, GroupLookup.NAME);
          }
        },
        () ->
            SessionUtils.doWithoutCommit(
                GroupRoleRelMapper.class,
                mapper -> mapper.softDeleteGroupRoleRelByGroupId(groupId)),
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper ->
                    mapper.softDeleteOwnerRelByOwnerIdAndType(
                        groupId, Entity.EntityType.GROUP.name())));
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "updateGroup")
  public <E extends Entity & HasIdentifier> GroupEntity updateGroup(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    AuthorizationUtils.checkGroup(identifier);

    NameIdentifier metalakeIdent = NameIdentifier.of(NameIdentifierUtil.getMetalake(identifier));
    Long metalakeId = EntityIdService.getEntityId(metalakeIdent, Entity.EntityType.METALAKE);

    GroupPO oldGroupPO = getGroupPOByMetalakeIdAndName(metalakeId, identifier.name());
    List<RolePO> rolePOs =
        RoleMetaService.getInstance().listRolesByGroupId(oldGroupPO.getGroupId());
    GroupEntity oldGroupEntity =
        POConverters.fromGroupPO(oldGroupPO, rolePOs, identifier.namespace());

    GroupEntity newEntity = (GroupEntity) updater.apply((E) oldGroupEntity);
    Preconditions.checkArgument(
        Objects.equals(oldGroupEntity.id(), newEntity.id()),
        "The updated group entity id: %s should be same with the group entity id before: %s",
        newEntity.id(),
        oldGroupEntity.id());

    Set<Long> oldRoleIds =
        oldGroupEntity.roleIds() == null
            ? Sets.newHashSet()
            : Sets.newHashSet(oldGroupEntity.roleIds());
    Set<Long> newRoleIds =
        newEntity.roleIds() == null ? Sets.newHashSet() : Sets.newHashSet(newEntity.roleIds());

    Set<Long> insertRoleIds = Sets.difference(newRoleIds, oldRoleIds);
    Set<Long> deleteRoleIds = Sets.difference(oldRoleIds, newRoleIds);

    // Every update runs the compare-and-set, including one that leaves the roles untouched. The
    // short-circuit that used to return early here would skip the version check, so a caller whose
    // snapshot was already stale would be told the update succeeded. It also has to run because a
    // metadata-only change, such as the audit info, still has to be written.
    try {
      SessionUtils.doMultipleWithCommit(
          () -> {
            int updated =
                SessionUtils.getWithoutCommit(
                    GroupMetaMapper.class,
                    mapper ->
                        mapper.updateGroupMeta(
                            POConverters.updateGroupPOWithVersion(oldGroupPO, newEntity),
                            oldGroupPO));
            if (updated == 0) {
              throw groupWriteFailure(identifier, oldGroupPO, GroupLookup.NAME);
            }
          },
          () -> {
            if (insertRoleIds.isEmpty()) {
              return;
            }
            SessionUtils.doWithoutCommit(
                GroupRoleRelMapper.class,
                mapper ->
                    mapper.batchInsertGroupRoleRel(
                        POConverters.initializeGroupRoleRelsPOWithVersion(
                            newEntity, Lists.newArrayList(insertRoleIds))));
          },
          () -> {
            if (deleteRoleIds.isEmpty()) {
              return;
            }
            SessionUtils.doWithoutCommit(
                GroupRoleRelMapper.class,
                mapper ->
                    mapper.softDeleteGroupRoleRelByGroupAndRoles(
                        newEntity.id(), Lists.newArrayList(deleteRoleIds)));
          },
          () ->
              SessionUtils.doWithoutCommit(
                  GroupMetaMapper.class,
                  mapper -> mapper.touchGroupUpdatedAt(oldGroupPO.getGroupId())));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.GROUP, newEntity.nameIdentifier().toString());
      throw re;
    }
    return newEntity;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listGroupsByNamespace")
  public List<GroupEntity> listGroupsByNamespace(Namespace namespace, boolean allFields) {
    AuthorizationUtils.checkGroupNamespace(namespace);
    String metalakeName = namespace.level(0);

    if (allFields) {
      NameIdentifier metalakeIdent = NameIdentifier.of(metalakeName);
      long metalakeId = EntityIdService.getEntityId(metalakeIdent, Entity.EntityType.METALAKE);
      List<ExtendedGroupPO> groupPOs =
          SessionUtils.getWithoutCommit(
              GroupMetaMapper.class, mapper -> mapper.listExtendedGroupPOsByMetalakeId(metalakeId));
      return groupPOs.stream()
          .map(
              po ->
                  POConverters.fromExtendedGroupPO(
                      po, AuthorizationUtils.ofGroupNamespace(metalakeName)))
          .collect(Collectors.toList());
    } else {
      List<GroupPO> groupPOs =
          SessionUtils.getWithoutCommit(
              GroupMetaMapper.class, mapper -> mapper.listGroupPOsByMetalake(metalakeName));
      return groupPOs.stream()
          .map(
              po ->
                  POConverters.fromGroupPO(
                      po,
                      Collections.emptyList(),
                      AuthorizationUtils.ofGroupNamespace(metalakeName)))
          .collect(Collectors.toList());
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteGroupMetasByLegacyTimeline")
  public int deleteGroupMetasByLegacyTimeline(long legacyTimeline, int limit) {
    int[] groupDeletedCount = new int[] {0};
    int[] groupRoleRelDeletedCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            groupDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    GroupMetaMapper.class,
                    mapper -> mapper.deleteGroupMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            groupRoleRelDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    GroupRoleRelMapper.class,
                    mapper ->
                        mapper.deleteGroupRoleRelMetasByLegacyTimeline(legacyTimeline, limit)));

    return groupDeletedCount[0] + groupRoleRelDeletedCount[0];
  }

  private GroupPO getGroupPOByMetalakeNameAndExternalId(String metalakeName, String externalId) {
    GroupPO groupPO =
        SessionUtils.getWithoutCommit(
            GroupMetaMapper.class,
            mapper -> mapper.selectGroupMetaByMetalakeNameAndExternalId(metalakeName, externalId));

    if (groupPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.GROUP.name().toLowerCase(),
          externalId);
    }
    return groupPO;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getGroupByExternalId")
  public GroupEntity getGroupByExternalId(NameIdentifier ident) {
    AuthorizationUtils.checkGroupExternalId(ident);
    String metalake = ident.namespace().level(0);
    String externalId = ident.name();
    GroupPO groupPO = getGroupPOByMetalakeNameAndExternalId(metalake, externalId);
    List<RolePO> rolePOs = RoleMetaService.getInstance().listRolesByGroupId(groupPO.getGroupId());
    return POConverters.fromGroupPO(
        groupPO, rolePOs, AuthorizationUtils.ofGroupNamespace(metalake));
  }

  private GroupPO getGroupPOByMetalakeNameAndId(String metalakeName, Long groupId) {
    GroupPO groupPO =
        SessionUtils.getWithoutCommit(
            GroupMetaMapper.class,
            mapper -> mapper.selectGroupMetaByMetalakeNameAndId(metalakeName, groupId));

    if (groupPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.GROUP.name().toLowerCase(),
          String.valueOf(groupId));
    }
    return groupPO;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getGroupById")
  public GroupEntity getGroupById(String metalake, long groupId) {
    GroupPO groupPO = getGroupPOByMetalakeNameAndId(metalake, groupId);
    List<RolePO> rolePOs = RoleMetaService.getInstance().listRolesByGroupId(groupPO.getGroupId());
    return POConverters.fromGroupPO(
        groupPO, rolePOs, AuthorizationUtils.ofGroupNamespace(metalake));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateGroupById")
  public <E extends Entity & HasIdentifier> GroupEntity updateGroupById(
      String metalake, long groupId, Function<E, E> updater) throws IOException {
    GroupPO oldGroupPO = getGroupPOByMetalakeNameAndId(metalake, groupId);
    List<RolePO> rolePOs =
        RoleMetaService.getInstance().listRolesByGroupId(oldGroupPO.getGroupId());
    GroupEntity oldEntity =
        POConverters.fromGroupPO(
            oldGroupPO, rolePOs, AuthorizationUtils.ofGroupNamespace(metalake));
    GroupEntity newEntity = (GroupEntity) updater.apply((E) oldEntity);
    Preconditions.checkArgument(
        Objects.equals(oldEntity.id(), newEntity.id()),
        "The updated group entity id: %s should be same with the group entity id before: %s",
        newEntity.id(),
        oldEntity.id());

    try {
      SessionUtils.doMultipleWithCommit(
          () -> {
            int updated =
                SessionUtils.getWithoutCommit(
                    GroupMetaMapper.class,
                    mapper ->
                        mapper.updateGroupMeta(
                            POConverters.updateGroupPOWithVersion(oldGroupPO, newEntity),
                            oldGroupPO));
            if (updated == 0) {
              NameIdentifier groupIdIdentifier =
                  AuthorizationUtils.ofGroup(metalake, String.valueOf(groupId));
              throw groupWriteFailure(groupIdIdentifier, oldGroupPO, GroupLookup.ID);
            }
          },
          () ->
              SessionUtils.doWithoutCommit(
                  GroupMetaMapper.class,
                  mapper -> mapper.touchGroupUpdatedAt(oldGroupPO.getGroupId())));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.GROUP, newEntity.nameIdentifier().toString());
      throw re;
    }
    return newEntity;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteGroupById")
  public boolean deleteGroupById(String metalake, long groupId) {
    GroupPO groupPO;
    try {
      groupPO = getGroupPOByMetalakeNameAndId(metalake, groupId);
    } catch (NoSuchEntityException e) {
      return false;
    }

    NameIdentifier identifier = AuthorizationUtils.ofGroup(metalake, groupPO.getGroupName());

    // Starts false so that any path that does not reach the child cleanup reports "nothing was
    // deleted here" rather than claiming a delete it did not perform.
    AtomicBoolean deletedGroup = new AtomicBoolean(false);
    SessionUtils.doMultipleWithCommit(
        () -> {
          int deleted =
              SessionUtils.getWithoutCommit(
                  GroupMetaMapper.class,
                  mapper ->
                      mapper.softDeleteGroupMetaByGroupId(groupId, groupPO.getCurrentVersion()));
          if (deleted == 0) {
            // The compare-and-set matched no row for one of two reasons. Either the row is already
            // gone, and a delete that has nothing left to delete is a no-op rather than an error,
            // or the row is still there under a newer version, which is a genuine conflict.
            if (getGroupPOByIdForUpdate(groupId) == null) {
              return;
            }
            throw ExceptionUtils.concurrentModification(Entity.EntityType.GROUP, identifier);
          }

          deletedGroup.set(true);
          SessionUtils.doWithoutCommit(
              GroupRoleRelMapper.class, mapper -> mapper.softDeleteGroupRoleRelByGroupId(groupId));
          SessionUtils.doWithoutCommit(
              OwnerMetaMapper.class,
              mapper ->
                  mapper.softDeleteOwnerRelByOwnerIdAndType(
                      groupId, Entity.EntityType.GROUP.name()));
        });
    return deletedGroup.get();
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "countGroupsByMetalake")
  public long countGroupsByMetalake(String metalakeName) {
    Long count =
        SessionUtils.getWithoutCommit(
            GroupMetaMapper.class, mapper -> mapper.countGroupMetasByMetalakeName(metalakeName));
    return count == null ? 0L : count;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listGroupsByMetalakePaginated")
  public PagedResult<GroupEntity> listGroupsByMetalakePaginated(
      String metalakeName, int offset, int limit) {
    Preconditions.checkArgument(offset >= 0, "offset must be >= 0");
    Preconditions.checkArgument(limit >= 0, "limit must be >= 0");

    long totalCount = countGroupsByMetalake(metalakeName);
    if (limit == 0 || offset >= totalCount) {
      return new PagedResult<>(totalCount, Collections.emptyList());
    }

    List<ExtendedGroupPO> groupPOs =
        SessionUtils.getWithoutCommit(
            GroupMetaMapper.class,
            mapper ->
                mapper.listExtendedGroupPOsByMetalakeNamePaginated(metalakeName, offset, limit));
    List<GroupEntity> groups =
        groupPOs.stream()
            .map(
                po ->
                    POConverters.fromExtendedGroupPO(
                        po, AuthorizationUtils.ofGroupNamespace(metalakeName)))
            .collect(Collectors.toList());
    return new PagedResult<>(totalCount, groups);
  }

  /**
   * Holds the parent metalake row for the rest of the transaction, so the group cannot be created
   * under a metalake that is going away.
   *
   * <p>The lock is shared, not exclusive: many groups can be created under the same metalake at the
   * same time. Dropping a metalake takes an exclusive lock on this row, so a drop and a create
   * cannot overlap. Whoever gets the row first wins, and the loser either sees the metalake gone or
   * inserts under a metalake that is still there.
   *
   * <p>The name is compared again because the ID alone cannot tell a rename apart: the caller
   * looked the metalake up by name, so a renamed row means the name in the request no longer
   * exists.
   *
   * <p>The metalake's version is deliberately not compared, matching {@code CatalogMetaService}.
   * Holding the row is what makes the create safe. An unrelated metalake edit that commits in
   * between bumps the version without making this create wrong, so comparing it would reject the
   * create for no reason.
   */
  private void lockMetalakeForGroupCreate(MetalakePO observedMetalakePO) {
    MetalakePO currentMetalakePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class,
            mapper -> mapper.selectMetalakeMetaByIdForShare(observedMetalakePO.getMetalakeId()));
    if (currentMetalakePO == null
        || !Objects.equals(
            currentMetalakePO.getMetalakeName(), observedMetalakePO.getMetalakeName())) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          observedMetalakePO.getMetalakeName());
    }
  }

  private RuntimeException groupWriteFailure(
      NameIdentifier identifier, GroupPO observedGroupPO, GroupLookup lookup) {
    // Sessions run at READ_COMMITTED, so a plain read would already see the latest committed row.
    // The locking read additionally waits for a writer that is still in flight, so a rename or
    // delete that has not committed yet is classified as not-found instead of as a stale-version
    // conflict. The lock is taken on the error path of a transaction that is about to roll back.
    GroupPO currentGroupPO = getGroupPOByIdForUpdate(observedGroupPO.getGroupId());
    boolean missing =
        currentGroupPO == null
            || !Objects.equals(currentGroupPO.getMetalakeId(), observedGroupPO.getMetalakeId());
    if (!missing && lookup == GroupLookup.NAME) {
      missing = !Objects.equals(currentGroupPO.getGroupName(), observedGroupPO.getGroupName());
    }
    if (missing) {
      return new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.GROUP.name().toLowerCase(),
          identifier.name());
    }
    return ExceptionUtils.concurrentModification(Entity.EntityType.GROUP, identifier);
  }

  private GroupPO getGroupPOByIdForUpdate(long groupId) {
    return SessionUtils.getWithoutCommit(
        GroupMetaMapper.class, mapper -> mapper.selectGroupMetaByIdForUpdate(groupId));
  }

  /**
   * How the caller addressed the group, which decides what counts as "the same group" when a failed
   * compare-and-set is classified. A caller that used the name is looking for that name, so a
   * rename means the group it asked for is gone. A caller that used the ID addressed the row
   * itself, so a rename leaves it addressing the same group and only the metalake has to still
   * match.
   */
  private enum GroupLookup {
    NAME,
    ID
  }
}
