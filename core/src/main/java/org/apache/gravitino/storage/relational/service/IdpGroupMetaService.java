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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.storage.relational.mapper.IdpGroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.IdpGroupUserRelMapper;
import org.apache.gravitino.storage.relational.po.IdpGroupPO;
import org.apache.gravitino.storage.relational.po.IdpGroupUserRelPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** The service class for group metadata. It provides the basic database operations for group. */
public class IdpGroupMetaService {
  private static final IdpGroupMetaService INSTANCE = new IdpGroupMetaService();

  public static IdpGroupMetaService getInstance() {
    return INSTANCE;
  }

  private IdpGroupMetaService() {}

  public Optional<IdpGroupPO> findGroup(String groupName) {
    return Optional.ofNullable(
        SessionUtils.getWithoutCommit(
            IdpGroupMetaMapper.class, mapper -> mapper.selectIdpGroup(groupName)));
  }

  public List<String> listUserNames(String groupName) {
    Optional<IdpGroupPO> group = findGroup(groupName);
    if (!group.isPresent()) {
      return Collections.emptyList();
    }

    return SessionUtils.getWithoutCommit(
        IdpGroupUserRelMapper.class,
        mapper -> mapper.selectUserNamesByGroupId(group.get().getGroupId()));
  }

  public List<String> listGroupNames(Long userId) {
    return SessionUtils.getWithoutCommit(
        IdpGroupUserRelMapper.class, mapper -> mapper.selectGroupNamesByUserId(userId));
  }

  public void createGroup(IdpGroupPO groupPO) {
    SessionUtils.doWithCommit(IdpGroupMetaMapper.class, mapper -> mapper.insertIdpGroup(groupPO));
  }

  public boolean deleteGroup(IdpGroupPO groupPO, Long deletedAt, String auditInfo) {
    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                IdpGroupMetaMapper.class,
                mapper -> mapper.softDeleteIdpGroup(groupPO.getGroupId(), deletedAt, auditInfo)),
        () ->
            SessionUtils.doWithoutCommit(
                IdpGroupUserRelMapper.class,
                mapper ->
                    mapper.softDeleteGroupUsersByGroupId(
                        groupPO.getGroupId(), deletedAt, auditInfo)));
    return true;
  }

  public List<Long> selectRelatedUserIds(Long groupId, List<Long> userIds) {
    return SessionUtils.getWithoutCommit(
        IdpGroupUserRelMapper.class, mapper -> mapper.selectRelatedUserIds(groupId, userIds));
  }

  public void addUsersToGroup(List<IdpGroupUserRelPO> relations) {
    if (relations.isEmpty()) {
      return;
    }
    SessionUtils.doWithCommit(
        IdpGroupUserRelMapper.class, mapper -> mapper.batchInsertIdpGroupUsers(relations));
  }

  public void removeUsersFromGroup(
      Long groupId, List<Long> userIds, Long deletedAt, String auditInfo) {
    if (userIds.isEmpty()) {
      return;
    }

    SessionUtils.doWithCommit(
        IdpGroupUserRelMapper.class,
        mapper -> mapper.softDeleteIdpGroupUsers(groupId, userIds, deletedAt, auditInfo));
  }

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
