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
import org.apache.gravitino.storage.relational.po.IdpUserMeta;
import org.apache.gravitino.storage.relational.provider.IdpMetaProviderLoader;
import org.apache.gravitino.storage.relational.provider.IdpUserMetaProvider;

/** Core facade for built-in IdP user metadata operations via plugin implementation. */
public class IdpUserMetaService<U extends IdpUserMeta> implements IdpUserMetaProvider<U> {
  private static final IdpUserMetaService<?> INSTANCE = new IdpUserMetaService<>();

  /**
   * Returns the singleton IdP user metadata service.
   *
   * @param <U> the user metadata type
   * @return the singleton service
   */
  @SuppressWarnings("unchecked")
  public static <U extends IdpUserMeta> IdpUserMetaService<U> getInstance() {
    return (IdpUserMetaService<U>) INSTANCE;
  }

  private IdpUserMetaService() {}

  /**
   * Find a built-in IdP user by name.
   *
   * @param userName the user name
   * @return the matched user if present
   */
  @Override
  public Optional<U> findUser(String userName) {
    return provider().findUser(userName);
  }

  /**
   * Find built-in IdP users by names.
   *
   * @param userNames the user names
   * @return the matched users
   */
  @Override
  public List<U> findUsers(List<String> userNames) {
    return provider().findUsers(userNames);
  }

  /**
   * List the groups of a built-in IdP user.
   *
   * @param userName the user name
   * @return the group names
   */
  @Override
  public List<String> listGroupNames(String userName) {
    return provider().listGroupNames(userName);
  }

  /**
   * Create a built-in IdP user.
   *
   * @param userMeta the user metadata
   */
  @Override
  public void createUser(U userMeta) {
    provider().createUser(userMeta);
  }

  /**
   * Update the password of a built-in IdP user.
   *
   * @param userMeta the current user metadata
   * @param passwordHash the new password hash
   * @param nextVersion the next version
   */
  @Override
  public void updatePassword(U userMeta, String passwordHash, Long nextVersion) {
    provider().updatePassword(userMeta, passwordHash, nextVersion);
  }

  /**
   * Soft delete a built-in IdP user.
   *
   * @param userMeta the user metadata
   * @param deletedAt the deletion timestamp
   * @return true if the delete succeeded
   */
  @Override
  public boolean deleteUser(U userMeta, Long deletedAt) {
    return provider().deleteUser(userMeta, deletedAt);
  }

  /**
   * Hard deletes legacy built-in IdP user metadata records.
   *
   * @param legacyTimeline delete records older than this timeline
   * @param limit maximum number of records to delete per invocation
   * @return the number of deleted records
   */
  @Override
  public int deleteUserMetasByLegacyTimeline(long legacyTimeline, int limit) {
    return provider().deleteUserMetasByLegacyTimeline(legacyTimeline, limit);
  }

  @SuppressWarnings("unchecked")
  private IdpUserMetaProvider<U> provider() {
    return (IdpUserMetaProvider<U>) IdpMetaProviderLoader.loadService(IdpUserMetaProvider.class);
  }
}
