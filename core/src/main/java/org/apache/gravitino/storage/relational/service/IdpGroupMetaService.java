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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

/** Core service contract for built-in IdP group metadata operations. */
public interface IdpGroupMetaService<G, R> {

  /**
   * Returns the IdP group metadata service implementation from the runtime classpath.
   *
   * @param <G> the group metadata type
   * @param <R> the group-user relation metadata type
   * @return the service implementation
   */
  @SuppressWarnings("unchecked")
  static <G, R> IdpGroupMetaService<G, R> getInstance() {
    return (IdpGroupMetaService<G, R>) loadService();
  }

  /**
   * Find a built-in IdP group by name.
   *
   * @param groupName the group name
   * @return the matched group if present
   */
  Optional<G> findGroup(String groupName);

  /**
   * List the users of a built-in IdP group.
   *
   * @param groupName the group name
   * @return the user names
   */
  List<String> listUserNames(String groupName);

  /**
   * Create a built-in IdP group.
   *
   * @param groupMeta the group metadata
   */
  void createGroup(G groupMeta);

  /**
   * Soft delete a built-in IdP group.
   *
   * @param groupMeta the group metadata
   * @param deletedAt the deletion timestamp
   * @return true if the delete succeeded
   */
  boolean deleteGroup(G groupMeta, Long deletedAt);

  /**
   * Select user ids already related to the group.
   *
   * @param groupId the group id
   * @param userIds the candidate user ids
   * @return the related user ids
   */
  List<Long> selectRelatedUserIds(Long groupId, List<Long> userIds);

  /**
   * Add users to a built-in IdP group.
   *
   * @param relations the group-user relations
   */
  void addUsersToGroup(List<R> relations);

  /**
   * Remove users from a built-in IdP group.
   *
   * @param groupId the group id
   * @param userIds the user ids to remove
   * @param deletedAt the deletion timestamp
   */
  void removeUsersFromGroup(Long groupId, List<Long> userIds, Long deletedAt);

  /**
   * Hard deletes legacy built-in IdP group metadata records.
   *
   * @param legacyTimeline delete records older than this timeline
   * @param limit maximum number of records to delete per invocation
   * @return the number of deleted records
   */
  int deleteGroupMetasByLegacyTimeline(long legacyTimeline, int limit);

  private static IdpGroupMetaService<?, ?> loadService() {
    List<IdpGroupMetaService<?, ?>> services = new ArrayList<>();
    for (IdpGroupMetaService<?, ?> service : ServiceLoader.load(IdpGroupMetaService.class)) {
      services.add(service);
    }

    if (services.isEmpty()) {
      throw new IllegalStateException("No IdpGroupMetaService implementation found");
    }

    if (services.size() > 1) {
      throw new IllegalStateException("Multiple IdpGroupMetaService implementations found");
    }

    return services.get(0);
  }
}
