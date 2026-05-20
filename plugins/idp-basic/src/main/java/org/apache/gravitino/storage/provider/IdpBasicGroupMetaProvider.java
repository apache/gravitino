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
package org.apache.gravitino.storage.provider;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.idp.storage.mapper.IdpGroupMetaMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserGroupRelMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserMetaMapper;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.idp.storage.po.IdpUserGroupRelPO;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.storage.relational.service.IdpGroupMetaService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** The provider class for group metadata. It provides the basic database operations for group. */
public class IdpBasicGroupMetaProvider
    implements IdpGroupMetaService<IdpGroupPO, IdpUserGroupRelPO> {
  private static final IdpBasicGroupMetaProvider INSTANCE = new IdpBasicGroupMetaProvider();

  public static IdpBasicGroupMetaProvider getInstance() {
    return INSTANCE;
  }

  public IdpBasicGroupMetaProvider() {}

  @Override
  public Optional<IdpGroupPO> findGroup(String groupName) {
    return Optional.ofNullable(
        SessionUtils.getWithoutCommit(
            IdpGroupMetaMapper.class, mapper -> mapper.selectIdpGroup(groupName)));
  }

  @Override
  public List<String> listUserNames(String groupName) {
    return SessionUtils.getWithoutCommit(
        IdpUserGroupRelMapper.class, mapper -> mapper.selectUsernamesByGroupName(groupName));
  }

  @Override
  public void createGroup(IdpGroupPO groupPO) {
    SessionUtils.doWithCommit(IdpGroupMetaMapper.class, mapper -> mapper.insertIdpGroup(groupPO));
  }

  @Override
  public boolean deleteGroup(IdpGroupPO groupPO, Long deletedAt) {
    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                IdpGroupMetaMapper.class,
                mapper -> mapper.softDeleteIdpGroup(groupPO.getGroupId())),
        () ->
            SessionUtils.doWithoutCommit(
                IdpUserGroupRelMapper.class,
                mapper -> mapper.softDeleteRelationsByGroupName(groupPO.getGroupName())));
    return true;
  }

  @Override
  public List<Long> selectRelatedUserIds(Long groupId, List<Long> userIds) {
    if (userIds.isEmpty()) {
      return Collections.emptyList();
    }

    return SessionUtils.getWithoutCommit(
        IdpGroupMetaMapper.class,
        groupMapper -> {
          IdpGroupPO group =
              groupMapper.selectIdpGroups(Collections.emptyList()).stream()
                  .filter(groupPO -> groupId.equals(groupPO.getGroupId()))
                  .findFirst()
                  .orElse(null);
          if (group == null) {
            return Collections.emptyList();
          }

          Set<String> memberUsernames =
              SessionUtils.getWithoutCommit(
                      IdpUserGroupRelMapper.class,
                      relMapper -> relMapper.selectUsernamesByGroupName(group.getGroupName()))
                  .stream()
                  .collect(Collectors.toSet());
          if (memberUsernames.isEmpty()) {
            return Collections.emptyList();
          }

          Set<Long> candidateUserIds = Set.copyOf(userIds);
          return SessionUtils.getWithoutCommit(
                  IdpUserMetaMapper.class,
                  userMapper -> userMapper.selectIdpUsers(List.copyOf(memberUsernames)))
              .stream()
              .map(IdpUserPO::getUserId)
              .filter(candidateUserIds::contains)
              .collect(Collectors.toList());
        });
  }

  @Override
  public void addUsersToGroup(List<IdpUserGroupRelPO> relations) {
    if (relations.isEmpty()) {
      return;
    }
    SessionUtils.doWithCommit(
        IdpUserGroupRelMapper.class, mapper -> mapper.batchInsertRelations(relations));
  }

  @Override
  public void removeUsersFromGroup(Long groupId, List<Long> userIds, Long deletedAt) {
    if (userIds.isEmpty()) {
      return;
    }

    SessionUtils.doWithCommit(
        IdpGroupMetaMapper.class,
        groupMapper -> {
          IdpGroupPO group =
              groupMapper.selectIdpGroups(Collections.emptyList()).stream()
                  .filter(groupPO -> groupId.equals(groupPO.getGroupId()))
                  .findFirst()
                  .orElse(null);
          if (group == null) {
            return;
          }

          Set<Long> userIdsToRemove = Set.copyOf(userIds);
          List<String> usernames =
              SessionUtils.getWithoutCommit(
                      IdpUserMetaMapper.class,
                      userMapper -> userMapper.selectIdpUsers(Collections.emptyList()))
                  .stream()
                  .filter(user -> userIdsToRemove.contains(user.getUserId()))
                  .map(IdpUserPO::getUserName)
                  .collect(Collectors.toList());
          if (usernames.isEmpty()) {
            return;
          }

          SessionUtils.doWithoutCommit(
              IdpUserGroupRelMapper.class,
              relMapper -> relMapper.softDeleteRelations(group.getGroupName(), usernames));
        });
  }

  @Override
  public int deleteGroupMetasByLegacyTimeline(long legacyTimeline, int limit) {
    int[] groupDeletedCount = new int[] {0};
    int[] groupUserRelDeletedCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            groupDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    IdpGroupMetaMapper.class,
                    mapper -> mapper.deleteIdpGroupMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            groupUserRelDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    IdpUserGroupRelMapper.class,
                    mapper ->
                        mapper.deleteIdpUserGroupRelMetasByLegacyTimeline(legacyTimeline, limit)));

    return groupDeletedCount[0] + groupUserRelDeletedCount[0];
  }
}
