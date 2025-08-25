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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.storage.relational.mapper.GroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.GroupRoleRelMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.po.ExtendedGroupPO;
import org.apache.gravitino.storage.relational.po.GroupPO;
import org.apache.gravitino.storage.relational.po.GroupRoleRelPO;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

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

  public GroupEntity getGroupByIdentifier(NameIdentifier identifier) {
    AuthorizationUtils.checkGroup(identifier);

    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
    GroupPO groupPO = getGroupPOByMetalakeIdAndName(metalakeId, identifier.name());
    List<RolePO> rolePOs = RoleMetaService.getInstance().listRolesByGroupId(groupPO.getGroupId());

    return POConverters.fromGroupPO(groupPO, rolePOs, identifier.namespace());
  }

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

  public void insertGroup(GroupEntity groupEntity, boolean overwritten) throws IOException {
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
                mapper -> mapper.softDeleteGroupRoleRelByGroupId(groupId)),
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper ->
                    mapper.softDeleteOwnerRelByOwnerIdAndType(
                        groupId, Entity.EntityType.GROUP.name())));
    return true;
  }

  public <E extends Entity & HasIdentifier> GroupEntity updateGroup(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
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

  public List<GroupEntity> listGroupsByNamespace(Namespace namespace, boolean allFields) {
    AuthorizationUtils.checkGroupNamespace(namespace);
    String metalakeName = namespace.level(0);

    if (allFields) {
      Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);
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
}
