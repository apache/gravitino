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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.storage.mapper.IdpUserGroupRelMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserMetaMapper;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.idp.storage.relational.utils.IdpExceptionUtils;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * The service class for built-in IdP user metadata. It provides the basic database operations for
 * user.
 */
public class IdpUserMetaService {
  private static final IdpUserMetaService INSTANCE = new IdpUserMetaService();

  private IdpUserMetaService() {}

  public static IdpUserMetaService getInstance() {
    return INSTANCE;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getIdpUserByUsername")
  public IdpUserPO getIdpUserByUsername(String username) {
    IdpUserPO userPO =
        SessionUtils.getWithoutCommit(
            IdpUserMetaMapper.class, mapper -> mapper.selectIdpUser(username));
    if (userPO == null) {
      throw new NotFoundException("IdP user not found: %s", username);
    }
    return userPO;
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
  public void insertIdpUser(IdpUserPO userPO) throws IOException {
    try {
      SessionUtils.doWithCommit(IdpUserMetaMapper.class, mapper -> mapper.insertIdpUser(userPO));
    } catch (RuntimeException re) {
      IdpExceptionUtils.checkSQLException(re, "user", userPO.getUsername());
      throw re;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteIdpUser")
  public boolean deleteIdpUser(String username) {
    int[] deletedCount = new int[] {0};
    SessionUtils.doMultipleWithCommit(
        () -> {
          Integer deleted =
              SessionUtils.getWithoutCommit(
                  IdpUserMetaMapper.class, mapper -> mapper.softDeleteIdpUser(username));
          deletedCount[0] = deleted == null ? 0 : deleted;
        },
        () -> {
          if (deletedCount[0] > 0) {
            SessionUtils.doWithoutCommit(
                IdpUserGroupRelMapper.class,
                mapper -> mapper.softDeleteRelationsByUsername(username));
          }
        });
    return deletedCount[0] > 0;
  }

  /**
   * Updates the password hash for an active user, following the core relational meta update flow:
   * load the current row, commit the update, and use the affected row count as the success signal.
   *
   * @param username username of the user
   * @param passwordHash new password hash to store
   * @return {@code true} if the user exists and the password hash was updated
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateIdpUserPassword")
  public boolean updateIdpUserPassword(String username, String passwordHash) {
    Integer updated =
        SessionUtils.doWithCommitAndFetchResult(
            IdpUserMetaMapper.class,
            mapper -> mapper.updateIdpUserPassword(username, passwordHash));
    return updated > 0;
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

  /**
   * Resolves active user ids for the given usernames in one query. Throws if any username is
   * missing.
   *
   * @param usernames usernames to resolve
   * @return username to user id map
   */
  Map<String, Long> resolveUserIdsByUsernames(List<String> usernames) {
    List<IdpUserPO> users =
        SessionUtils.getWithoutCommit(
            IdpUserMetaMapper.class, mapper -> mapper.selectIdpUsersByUsernames(usernames));
    Map<String, Long> userIds = new HashMap<>(users.size());
    for (IdpUserPO user : users) {
      userIds.put(user.getUsername(), user.getUserId());
    }
    for (String username : usernames) {
      if (!userIds.containsKey(username)) {
        throw new NotFoundException("IdP user not found: %s", username);
      }
    }
    return userIds;
  }
}
