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
package org.apache.gravitino.idp.storage.service;

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.storage.mapper.IdpGroupMetaMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserGroupRelMapper;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.idp.storage.po.IdpUserGroupRelPO;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * The service class for built-in IdP group metadata. It provides the basic database operations for
 * group.
 */
public class IdpGroupMetaService {
  private static final IdpGroupMetaService INSTANCE = new IdpGroupMetaService();

  private IdpGroupMetaService() {}

  public static IdpGroupMetaService getInstance() {
    return INSTANCE;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getIdpGroupByName")
  public IdpGroupPO getIdpGroupByName(String groupName) {
    return getIdpGroupPOByName(groupName);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listUsernamesByGroupName")
  public List<String> listUsernamesByGroupName(String groupName) {
    return SessionUtils.getWithoutCommit(
        IdpUserGroupRelMapper.class, mapper -> mapper.selectUsernamesByGroupName(groupName));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertIdpGroup")
  public void insertIdpGroup(IdpGroupPO groupPO) {
    SessionUtils.doWithCommit(IdpGroupMetaMapper.class, mapper -> mapper.insertIdpGroup(groupPO));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteIdpGroup")
  public boolean deleteIdpGroup(String groupName, boolean cascade) {
    if (!cascade && !listUsernamesByGroupName(groupName).isEmpty()) {
      throw new IllegalStateException(
          String.format("IdP group %s is not empty, use cascade=true to delete it", groupName));
    }

    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                IdpUserGroupRelMapper.class,
                mapper -> mapper.softDeleteRelationsByGroupName(groupName)),
        () ->
            SessionUtils.doWithoutCommit(
                IdpGroupMetaMapper.class, mapper -> mapper.softDeleteIdpGroup(groupName)));
    return true;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "addUsersToGroup")
  public void addUsersToGroup(String groupName, List<String> usernames) {
    IdpGroupPO group = getIdpGroupPOByName(groupName);
    Map<String, Long> userIds =
        IdpUserMetaService.getInstance().resolveUserIdsByUsernames(usernames);
    List<IdpUserGroupRelPO> relations = new ArrayList<>(usernames.size());
    for (String username : usernames) {
      relations.add(newUserGroupRelation(group.getGroupId(), userIds.get(username)));
    }

    SessionUtils.doWithCommit(
        IdpUserGroupRelMapper.class, mapper -> mapper.batchInsertRelations(relations));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "removeUsersFromGroup")
  public int removeUsersFromGroup(String groupName, List<String> usernames) {
    return SessionUtils.doWithCommitAndFetchResult(
        IdpUserGroupRelMapper.class, mapper -> mapper.softDeleteRelations(groupName, usernames));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteIdpGroupMetasByLegacyTimeline")
  public int deleteGroupMetasByLegacyTimeline(long legacyTimeline, int limit) {
    int[] groupDeletedCount = new int[] {0};
    int[] relDeletedCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            groupDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    IdpGroupMetaMapper.class,
                    mapper -> mapper.deleteIdpGroupMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            relDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    IdpUserGroupRelMapper.class,
                    mapper ->
                        mapper.deleteIdpUserGroupRelMetasByLegacyTimeline(legacyTimeline, limit)));

    return groupDeletedCount[0] + relDeletedCount[0];
  }

  private static IdpUserGroupRelPO newUserGroupRelation(long groupId, long userId) {
    return IdpUserGroupRelPO.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withUserId(userId)
        .withGroupId(groupId)
        .withCurrentVersion(1L)
        .withLastVersion(0L)
        .withDeletedAt(0L)
        .build();
  }

  private IdpGroupPO getIdpGroupPOByName(String groupName) {
    IdpGroupPO groupPO =
        SessionUtils.getWithoutCommit(
            IdpGroupMetaMapper.class, mapper -> mapper.selectIdpGroup(groupName));
    if (groupPO == null) {
      throw new NotFoundException("IdP group not found: %s", groupName);
    }
    return groupPO;
  }
}
