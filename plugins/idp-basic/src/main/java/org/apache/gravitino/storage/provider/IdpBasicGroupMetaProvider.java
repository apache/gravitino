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
import org.apache.gravitino.storage.relational.mapper.IdpGroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.IdpGroupUserRelMapper;
import org.apache.gravitino.storage.relational.po.IdpGroupPO;
import org.apache.gravitino.storage.relational.po.IdpGroupUserRelPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** The provider class for group metadata. It provides the basic database operations for group. */
public class IdpBasicGroupMetaProvider implements IdpGroupMetaProvider {
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
    Optional<IdpGroupPO> group = findGroup(groupName);
    if (!group.isPresent()) {
      return Collections.emptyList();
    }

    return SessionUtils.getWithoutCommit(
        IdpGroupUserRelMapper.class,
        mapper -> mapper.selectUserNamesByGroupId(group.get().getGroupId()));
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
                mapper -> mapper.softDeleteIdpGroup(groupPO.getGroupId(), deletedAt)),
        () ->
            SessionUtils.doWithoutCommit(
                IdpGroupUserRelMapper.class,
                mapper -> mapper.softDeleteGroupUsersByGroupId(groupPO.getGroupId(), deletedAt)));
    return true;
  }

  @Override
  public List<Long> selectRelatedUserIds(Long groupId, List<Long> userIds) {
    return SessionUtils.getWithoutCommit(
        IdpGroupUserRelMapper.class, mapper -> mapper.selectRelatedUserIds(groupId, userIds));
  }

  @Override
  public void addUsersToGroup(List<IdpGroupUserRelPO> relations) {
    if (relations.isEmpty()) {
      return;
    }
    SessionUtils.doWithCommit(
        IdpGroupUserRelMapper.class, mapper -> mapper.batchInsertIdpGroupUsers(relations));
  }

  @Override
  public void removeUsersFromGroup(Long groupId, List<Long> userIds, Long deletedAt) {
    if (userIds.isEmpty()) {
      return;
    }

    SessionUtils.doWithCommit(
        IdpGroupUserRelMapper.class,
        mapper -> mapper.softDeleteIdpGroupUsers(groupId, userIds, deletedAt));
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
                    IdpGroupUserRelMapper.class,
                    mapper ->
                        mapper.deleteIdpGroupUserRelMetasByLegacyTimeline(legacyTimeline, limit)));

    return groupDeletedCount[0] + groupUserRelDeletedCount[0];
  }
}
