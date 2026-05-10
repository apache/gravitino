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
package org.apache.gravitino.storage.relational.provider;

import java.util.List;
import java.util.Optional;
import org.apache.gravitino.storage.relational.po.IdpUserMeta;

/** SPI for built-in IdP user metadata operations. */
public interface IdpUserMetaProvider<U extends IdpUserMeta> {

  /**
   * Find a built-in IdP user by name.
   *
   * @param userName the user name
   * @return the matched user if present
   */
  Optional<U> findUser(String userName);

  /**
   * Find built-in IdP users by names.
   *
   * @param userNames the user names
   * @return the matched users
   */
  List<U> findUsers(List<String> userNames);

  /**
   * List the groups of a built-in IdP user.
   *
   * @param userName the user name
   * @return the group names
   */
  List<String> listGroupNames(String userName);

  /**
   * Create a built-in IdP user.
   *
   * @param userMeta the user metadata
   */
  void createUser(U userMeta);

  /**
   * Update the password of a built-in IdP user.
   *
   * @param userMeta the current user metadata
   * @param passwordHash the new password hash
   * @param nextVersion the next version
   */
  void updatePassword(U userMeta, String passwordHash, Long nextVersion);

  /**
   * Soft delete a built-in IdP user.
   *
   * @param userMeta the user metadata
   * @param deletedAt the deletion timestamp
   * @return true if the delete succeeded
   */
  boolean deleteUser(U userMeta, Long deletedAt);

  /**
   * Hard deletes legacy built-in IdP user metadata records.
   *
   * @param legacyTimeline delete records older than this timeline
   * @param limit maximum number of records to delete per invocation
   * @return the number of deleted records
   */
  int deleteUserMetasByLegacyTimeline(long legacyTimeline, int limit);
}
