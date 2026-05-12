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
package org.apache.gravitino.authorization;

import java.io.Closeable;
import java.util.List;
import org.apache.gravitino.storage.relational.po.IdpGroupMeta;
import org.apache.gravitino.storage.relational.po.IdpUserMeta;

/** Manager for built-in IdP user and group management APIs. */
public interface IdpManager extends Closeable {

  /**
   * Create a built-in IdP user.
   *
   * @param userName the user name to create
   * @param password the initial password for the user
   * @return the created user
   */
  IdpUserMeta createUser(String userName, String password);

  /**
   * Get a built-in IdP user.
   *
   * @param userName the user name
   * @return the requested user
   */
  IdpUserMeta getUser(String userName);

  /**
   * Delete a built-in IdP user.
   *
   * @param userName the user name to delete
   * @return true if the user was removed, false if the user does not exist
   */
  boolean deleteUser(String userName);

  /**
   * Reset the password of a built-in IdP user.
   *
   * @param userName the user name
   * @param password the new password
   * @return the updated user
   */
  IdpUserMeta resetPassword(String userName, String password);

  /**
   * Create a built-in IdP group.
   *
   * @param groupName the group name to create
   * @return the created group
   */
  IdpGroupMeta createGroup(String groupName);

  /**
   * Get a built-in IdP group.
   *
   * @param groupName the group name
   * @return the requested group
   */
  IdpGroupMeta getGroup(String groupName);

  /**
   * Delete a built-in IdP group without forcing removal of groups that still contain users.
   *
   * @param groupName the group name to delete
   * @return true if the group was removed, false if the group does not exist
   */
  default boolean deleteGroup(String groupName) {
    return deleteGroup(groupName, false);
  }

  /**
   * Delete a built-in IdP group.
   *
   * @param groupName the group name to delete
   * @param force whether to force deletion when the group still contains users
   * @return true if the group was removed, false if the group does not exist
   */
  boolean deleteGroup(String groupName, boolean force);

  /**
   * Add users to a built-in IdP group.
   *
   * @param groupName the group name
   * @param userNames the users to add
   * @return the updated group
   */
  IdpGroupMeta addUsersToGroup(String groupName, List<String> userNames);

  /**
   * Remove users from a built-in IdP group.
   *
   * @param groupName the group name
   * @param userNames the users to remove
   * @return the updated group
   */
  IdpGroupMeta removeUsersFromGroup(String groupName, List<String> userNames);

  /** Release any resources held by this manager. */
  @Override
  default void close() {}
}
