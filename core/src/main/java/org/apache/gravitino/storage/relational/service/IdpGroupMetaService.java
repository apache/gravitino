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

import java.util.List;
import java.util.Optional;
import org.apache.gravitino.storage.relational.po.IdpGroupMeta;
import org.apache.gravitino.storage.relational.po.IdpGroupUserRelMeta;
import org.apache.gravitino.storage.relational.provider.IdpGroupMetaProvider;
import org.apache.gravitino.storage.relational.provider.IdpMetaProviderLoader;

/** Core facade for built-in IdP group metadata operations via plugin implementation. */
public class IdpGroupMetaService<G extends IdpGroupMeta, R extends IdpGroupUserRelMeta>
    implements IdpGroupMetaProvider<G, R> {
  private static final IdpGroupMetaService<?, ?> INSTANCE = new IdpGroupMetaService<>();

  /**
   * Returns the singleton IdP group metadata service.
   *
   * @param <G> the group metadata type
   * @param <R> the group-user relation metadata type
   * @return the singleton service
   */
  @SuppressWarnings("unchecked")
  public static <G extends IdpGroupMeta, R extends IdpGroupUserRelMeta>
      IdpGroupMetaService<G, R> getInstance() {
    return (IdpGroupMetaService<G, R>) INSTANCE;
  }

  private IdpGroupMetaService() {}

  /**
   * Find a built-in IdP group by name.
   *
   * @param groupName the group name
   * @return the matched group if present
   */
  @Override
  public Optional<G> findGroup(String groupName) {
    return provider().findGroup(groupName);
  }

  /**
   * List the users of a built-in IdP group.
   *
   * @param groupName the group name
   * @return the user names
   */
  @Override
  public List<String> listUserNames(String groupName) {
    return provider().listUserNames(groupName);
  }

  /**
   * Create a built-in IdP group.
   *
   * @param groupMeta the group metadata
   */
  @Override
  public void createGroup(G groupMeta) {
    provider().createGroup(groupMeta);
  }

  /**
   * Soft delete a built-in IdP group.
   *
   * @param groupMeta the group metadata
   * @param deletedAt the deletion timestamp
   * @return true if the delete succeeded
   */
  @Override
  public boolean deleteGroup(G groupMeta, Long deletedAt) {
    return provider().deleteGroup(groupMeta, deletedAt);
  }

  /**
   * Select user ids already related to the group.
   *
   * @param groupId the group id
   * @param userIds the candidate user ids
   * @return the related user ids
   */
  @Override
  public List<Long> selectRelatedUserIds(Long groupId, List<Long> userIds) {
    return provider().selectRelatedUserIds(groupId, userIds);
  }

  /**
   * Add users to a built-in IdP group.
   *
   * @param relations the group-user relations
   */
  @Override
  public void addUsersToGroup(List<R> relations) {
    provider().addUsersToGroup(relations);
  }

  /**
   * Remove users from a built-in IdP group.
   *
   * @param groupId the group id
   * @param userIds the user ids to remove
   * @param deletedAt the deletion timestamp
   */
  @Override
  public void removeUsersFromGroup(Long groupId, List<Long> userIds, Long deletedAt) {
    provider().removeUsersFromGroup(groupId, userIds, deletedAt);
  }

  /**
   * Hard deletes legacy built-in IdP group metadata records.
   *
   * @param legacyTimeline delete records older than this timeline
   * @param limit maximum number of records to delete per invocation
   * @return the number of deleted records
   */
  @Override
  public int deleteGroupMetasByLegacyTimeline(long legacyTimeline, int limit) {
    return provider().deleteGroupMetasByLegacyTimeline(legacyTimeline, limit);
  }

  @SuppressWarnings("unchecked")
  private IdpGroupMetaProvider<G, R> provider() {
    return (IdpGroupMetaProvider<G, R>)
        IdpMetaProviderLoader.loadService(IdpGroupMetaProvider.class);
  }
}
