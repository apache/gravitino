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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.idp.exception.NoSuchEntityException;
import org.apache.gravitino.idp.storage.mapper.IdpGroupMetaMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserGroupRelMapper;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.idp.storage.po.IdpUserGroupRelPO;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * The service class for built-in IdP group metadata. It provides the basic database operations for
 * group.
 */
public class IdpGroupMetaService {
  private static final String IDP_GROUP_ENTITY_TYPE = "idp group";

  private static final IdpGroupMetaService INSTANCE = new IdpGroupMetaService();

  public static IdpGroupMetaService getInstance() {
    return INSTANCE;
  }

  private IdpGroupMetaService() {}

  private IdpGroupPO getIdpGroupPOByName(String groupName) {
    IdpGroupPO groupPO =
        SessionUtils.getWithoutCommit(
            IdpGroupMetaMapper.class, mapper -> mapper.selectIdpGroup(groupName));

    if (groupPO == null) {
      throw new NoSuchEntityException(IDP_GROUP_ENTITY_TYPE, groupName);
    }
    return groupPO;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getIdpGroupIdByName")
  public Long getIdpGroupIdByName(String groupName) {
    return getIdpGroupPOByName(groupName).getGroupId();
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getIdpGroupByName")
  public IdpGroupPO getIdpGroupByName(String groupName) {
    return getIdpGroupPOByName(groupName);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchGetIdpGroupsByName")
  public List<IdpGroupPO> batchGetIdpGroupsByName(List<String> groupNames) {
    Preconditions.checkNotNull(groupNames, "IdP group names cannot be null");
    return groupNames.stream().map(this::getIdpGroupPOByName).collect(Collectors.toList());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listIdpGroupsByUsername")
  public List<IdpGroupPO> listIdpGroupsByUsername(String username) {
    List<String> groupNames =
        SessionUtils.getWithoutCommit(
            IdpUserGroupRelMapper.class, mapper -> mapper.selectGroupNamesByUsername(username));
    if (groupNames.isEmpty()) {
      return Collections.emptyList();
    }
    return listIdpGroups(groupNames);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listIdpGroups")
  public List<IdpGroupPO> listIdpGroups(List<String> groupNames) {
    Preconditions.checkNotNull(groupNames, "IdP group names cannot be null");
    return SessionUtils.getWithoutCommit(
        IdpGroupMetaMapper.class, mapper -> mapper.selectIdpGroups(groupNames));
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
  public void insertIdpGroup(
      IdpGroupPO groupPO, List<IdpUserGroupRelPO> relations, boolean overwritten)
      throws IOException {
    try {
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  IdpGroupMetaMapper.class,
                  mapper -> {
                    if (overwritten) {
                      IdpGroupPO existing = mapper.selectIdpGroup(groupPO.getGroupName());
                      if (existing != null) {
                        mapper.softDeleteIdpGroup(existing.getGroupId());
                      }
                    }
                    mapper.insertIdpGroup(groupPO);
                  }),
          () ->
              SessionUtils.doWithoutCommit(
                  IdpUserGroupRelMapper.class,
                  mapper -> {
                    if (overwritten) {
                      mapper.softDeleteRelationsByGroupName(groupPO.getGroupName());
                    }
                    if (relations != null && !relations.isEmpty()) {
                      mapper.batchInsertRelations(relations);
                    }
                  }));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(re, Entity.EntityType.GROUP, groupPO.getGroupName());
      throw re;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteIdpGroup")
  public boolean deleteIdpGroup(String groupName) {
    Long groupId = getIdpGroupIdByName(groupName);

    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                IdpUserGroupRelMapper.class,
                mapper -> mapper.softDeleteRelationsByGroupName(groupName)),
        () ->
            SessionUtils.doWithoutCommit(
                IdpGroupMetaMapper.class, mapper -> mapper.softDeleteIdpGroup(groupId)));
    return true;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "softDeleteIdpUserGroupRelations")
  public int softDeleteIdpUserGroupRelations(String groupName, List<String> usernames) {
    Integer deleted =
        SessionUtils.getWithoutCommit(
            IdpUserGroupRelMapper.class,
            mapper -> mapper.softDeleteRelations(groupName, usernames));
    return deleted == null ? 0 : deleted;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteIdpGroupMetasByLegacyTimeline")
  public int deleteGroupMetasByLegacyTimeline(long legacyTimeline, int limit) {
    int[] groupDeletedCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            groupDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    IdpGroupMetaMapper.class,
                    mapper -> mapper.deleteIdpGroupMetasByLegacyTimeline(legacyTimeline, limit)));

    return groupDeletedCount[0];
  }
}
