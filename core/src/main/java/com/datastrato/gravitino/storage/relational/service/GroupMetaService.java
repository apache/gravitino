/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.authorization.AuthorizationUtils;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.GroupEntity;
import com.datastrato.gravitino.storage.relational.mapper.GroupMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.GroupRoleRelMapper;
import com.datastrato.gravitino.storage.relational.po.GroupPO;
import com.datastrato.gravitino.storage.relational.po.GroupRoleRelPO;
import com.datastrato.gravitino.storage.relational.po.RolePO;
import com.datastrato.gravitino.storage.relational.utils.ExceptionUtils;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

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

  private Long getGroupIdByMetalakeIdAndName(Long metalakeId, String groupName) {
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

  public GroupEntity getGroupByIdentifier(NameIdentifier identifier) {
    AuthorizationUtils.checkGroup(identifier);

    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
    GroupPO groupPO = getGroupPOByMetalakeIdAndName(metalakeId, identifier.name());
    List<RolePO> rolePOs = RoleMetaService.getInstance().listRolesByGroupId(groupPO.getGroupId());

    return POConverters.fromGroupPO(groupPO, rolePOs, identifier.namespace());
  }

  public void insertGroup(GroupEntity groupEntity, boolean overwritten) {
    try {
      AuthorizationUtils.checkGroup(groupEntity.nameIdentifier());

      Long metalakeId =
          MetalakeMetaService.getInstance().getMetalakeIdByName(groupEntity.namespace().level(0));
      GroupPO.Builder builder = GroupPO.builder().withMetalakeId(metalakeId);
      GroupPO GroupPO = POConverters.initializeGroupPOWithVersion(groupEntity, builder);

      List<Long> roleIds = Optional.ofNullable(groupEntity.roleIds()).orElse(Lists.newArrayList());
      List<GroupRoleRelPO> groupRoleRelPOS =
          POConverters.initializeGroupRoleRelsPOWithVersion(groupEntity, roleIds);

      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  GroupMetaMapper.class,
                  mapper -> {
                    if (overwritten) {
                      mapper.insertGroupMetaOnDuplicateKeyUpdate(GroupPO);
                    } else {
                      mapper.insertGroupMeta(GroupPO);
                    }
                  }),
          () -> {
            if (groupRoleRelPOS.isEmpty()) {
              return;
            }
            SessionUtils.doWithoutCommit(
                GroupRoleRelMapper.class,
                mapper -> {
                  if (overwritten) {
                    mapper.batchInsertGroupRoleRelOnDuplicateKeyUpdate(groupRoleRelPOS);
                  } else {
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

  public boolean deleteGroup(NameIdentifier identifier) {
    AuthorizationUtils.checkGroup(identifier);

    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
    Long groupId = getGroupIdByMetalakeIdAndName(metalakeId, identifier.name());

    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                GroupMetaMapper.class, mapper -> mapper.softDeleteGroupMetaByGroupId(groupId)),
        () ->
            SessionUtils.doWithoutCommit(
                GroupRoleRelMapper.class,
                mapper -> mapper.softDeleteGroupRoleRelByGroupId(groupId)));
    return true;
  }

  public <E extends Entity & HasIdentifier> GroupEntity updateGroup(
      NameIdentifier identifier, Function<E, E> updater) {
    AuthorizationUtils.checkGroup(identifier);

    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
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

    if (insertRoleIds.isEmpty() && deleteRoleIds.isEmpty()) {
      return newEntity;
    }
    try {
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  GroupMetaMapper.class,
                  mapper ->
                      mapper.updateGroupMeta(
                          POConverters.updateGroupPOWithVersion(oldGroupPO, newEntity),
                          oldGroupPO)),
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
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.GROUP, newEntity.nameIdentifier().toString());
      throw re;
    }
    return newEntity;
  }

  public int deleteGroupMetasByLegacyTimeLine(long legacyTimeLine, int limit) {
    int groupDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            GroupMetaMapper.class,
            mapper -> mapper.deleteGroupMetasByLegacyTimeLine(legacyTimeLine, limit));

    int groupRoleRelDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            GroupRoleRelMapper.class,
            mapper -> mapper.deleteGroupRoleRelMetasByLegacyTimeLine(legacyTimeLine, limit));
    return groupDeletedCount + groupRoleRelDeletedCount;
  }
}
