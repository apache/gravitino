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
import org.apache.gravitino.Entity;
import org.apache.gravitino.idp.exception.NoSuchEntityException;
import org.apache.gravitino.idp.storage.mapper.IdpUserGroupRelMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserMetaMapper;
import org.apache.gravitino.idp.storage.po.IdpUserGroupRelPO;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * The service class for built-in IdP user metadata. It provides the basic database operations for
 * user.
 */
public class IdpUserMetaService {
  private static final String IDP_USER_ENTITY_TYPE = "idp user";

  private static final IdpUserMetaService INSTANCE = new IdpUserMetaService();

  public static IdpUserMetaService getInstance() {
    return INSTANCE;
  }

  private IdpUserMetaService() {}

  private IdpUserPO getIdpUserPOByUsername(String username) {
    IdpUserPO userPO =
        SessionUtils.getWithoutCommit(
            IdpUserMetaMapper.class, mapper -> mapper.selectIdpUser(username));

    if (userPO == null) {
      throw new NoSuchEntityException(IDP_USER_ENTITY_TYPE, username);
    }
    return userPO;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getIdpUserIdByUsername")
  public Long getIdpUserIdByUsername(String username) {
    return getIdpUserPOByUsername(username).getUserId();
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getIdpUserByUsername")
  public IdpUserPO getIdpUserByUsername(String username) {
    return getIdpUserPOByUsername(username);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listIdpUsersByGroupName")
  public List<IdpUserPO> listIdpUsersByGroupName(String groupName) {
    List<String> usernames =
        SessionUtils.getWithoutCommit(
            IdpUserGroupRelMapper.class, mapper -> mapper.selectUsernamesByGroupName(groupName));
    if (usernames.isEmpty()) {
      return Collections.emptyList();
    }
    return listIdpUsers(usernames);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listIdpUsers")
  public List<IdpUserPO> listIdpUsers(List<String> usernames) {
    Preconditions.checkNotNull(usernames, "IdP usernames cannot be null");
    return SessionUtils.getWithoutCommit(
        IdpUserMetaMapper.class, mapper -> mapper.selectIdpUsers(usernames));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listGroupNamesByUsername")
  public List<String> listGroupNamesByUsername(String username) {
    return SessionUtils.getWithoutCommit(
        IdpUserGroupRelMapper.class, mapper -> mapper.selectGroupNamesByUsername(username));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertIdpUser")
  public void insertIdpUser(
      IdpUserPO userPO, List<IdpUserGroupRelPO> relations, boolean overwritten) throws IOException {
    try {
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  IdpUserMetaMapper.class,
                  mapper -> {
                    if (overwritten) {
                      IdpUserPO existing = mapper.selectIdpUser(userPO.getUsername());
                      if (existing != null) {
                        mapper.softDeleteIdpUser(existing.getUserId());
                      }
                    }
                    mapper.insertIdpUser(userPO);
                  }),
          () ->
              SessionUtils.doWithoutCommit(
                  IdpUserGroupRelMapper.class,
                  mapper -> {
                    if (overwritten) {
                      mapper.softDeleteRelationsByUsername(userPO.getUsername());
                    }
                    if (relations != null && !relations.isEmpty()) {
                      mapper.batchInsertRelations(relations);
                    }
                  }));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(re, Entity.EntityType.USER, userPO.getUsername());
      throw re;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteIdpUser")
  public boolean deleteIdpUser(String username) {
    Long userId = getIdpUserIdByUsername(username);

    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                IdpUserGroupRelMapper.class,
                mapper -> mapper.softDeleteRelationsByUsername(username)),
        () ->
            SessionUtils.doWithoutCommit(
                IdpUserMetaMapper.class, mapper -> mapper.softDeleteIdpUser(userId)));
    return true;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateIdpUserPassword")
  public boolean updateIdpUserPassword(String username, String passwordHash) {
    Long userId = getIdpUserIdByUsername(username);
    Integer updated =
        SessionUtils.getWithoutCommit(
            IdpUserMetaMapper.class, mapper -> mapper.updateIdpUserPassword(userId, passwordHash));
    return updated != null && updated > 0;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteIdpUserMetasByLegacyTimeline")
  public int deleteUserMetasByLegacyTimeline(long legacyTimeline, int limit) {
    int[] userDeletedCount = new int[] {0};
    int[] relDeletedCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            userDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    IdpUserMetaMapper.class,
                    mapper -> mapper.deleteIdpUserMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            relDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    IdpUserGroupRelMapper.class,
                    mapper ->
                        mapper.deleteIdpUserGroupRelMetasByLegacyTimeline(legacyTimeline, limit)));

    return userDeletedCount[0] + relDeletedCount[0];
  }
}
