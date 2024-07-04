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
package com.datastrato.gravitino.connector.authorization;

import com.datastrato.gravitino.authorization.Group;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.User;
import java.util.List;

/**
 * Interface for authorization User and Group hooks operation of the underlying access control
 * system.
 */
public interface AuthorizationUserHook {
  /**
   * Add a new User to the underlying access control system.
   *
   * @param user The user entity.
   * @return True if the add User was successfully added, false if the add User failed.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  Boolean onAddUser(User user) throws RuntimeException;

  /**
   * Removes a User from the underlying access control system.
   *
   * @param user The name of the User.
   * @return True if the User was successfully removed, false if the remove User failed.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  Boolean onRemoveUser(String user) throws RuntimeException;

  /**
   * Check if a user exists from the underlying access control system. Because User information is
   * already stored in the Gravition, so we don't need to get the User from the underlying access
   * control system. We only need to check if the User exists in the underlying access control
   * system.
   *
   * @param user The name of the User.
   * @return IF exist return true, else return false.
   * @throws RuntimeException If getting the User encounters underlying access control system
   *     issues.
   */
  Boolean onCheckUser(String user) throws RuntimeException;

  /**
   * Adds a new Group to the underlying access control system.
   *
   * @param group The name of the Group.
   * @return True if the add Group was successfully added, false if the add Group failed.
   * @throws RuntimeException If adding the Group encounters storage issues.
   */
  Boolean onAddGroup(String group) throws RuntimeException;

  /**
   * Removes a Group from the underlying access control system.
   *
   * @param group The name of the Group.
   * @return True if the remove Group was successfully removed, false if the remove Group was
   *     failed.
   * @throws RuntimeException If removing the Group encounters storage issues.
   */
  Boolean onRemoveGroup(String group) throws RuntimeException;

  /**
   * Check a Group if it exists from the underlying access control system. Because Group information
   * is already stored in the Gravition, so we don't need to get the Group from the underlying
   * access control system. We only need to check if the Group exists in the underlying access
   * control system.
   *
   * @param group The name of the Group.
   * @return If exist return true, else return false.
   * @throws RuntimeException If getting the Group encounters underlying access control system
   *     issues.
   */
  Boolean onCheckGroup(String group);

  /**
   * Grant roles to a user into the underlying access control system.
   *
   * @param user The entity of the User.
   * @param roles The entities of the Roles.
   * @return True if the Grant was successful, false if the Grant was failed.
   * @throws RuntimeException If granting roles to a user encounters storage issues.
   */
  Boolean onGrantRolesToUser(List<Role> roles, User user) throws RuntimeException;

  /**
   * Revoke roles from a user into the underlying access control system.
   *
   * @param user The entity of the User.
   * @param roles The entities of the Roles.
   * @return True if the revoke was successfully removed, false if the revoke failed.
   * @throws RuntimeException If revoking roles from a user encounters storage issues.
   */
  Boolean onRevokeRolesFromUser(List<Role> roles, User user) throws RuntimeException;

  /**
   * Grant roles to a group into the underlying access control system.
   *
   * @param group The entity of the Group.
   * @param roles The entities of the Roles.
   * @return True if the revoke was successfully removed, False if the revoke failed.
   * @throws RuntimeException If granting roles to a group encounters storage issues.
   */
  Boolean onGrantRolesToGroup(List<Role> roles, Group group) throws RuntimeException;

  /**
   * Revoke roles from a group from the underlying access control system.
   *
   * @param group The entity of the Group.
   * @param roles The entities of the Roles.
   * @return True if the revoke was successfully removed, False if the revoke failed.
   * @throws RuntimeException If revoking roles from a group encounters storage issues.
   */
  Boolean onRevokeRolesFromGroup(List<Role> roles, Group group) throws RuntimeException;
}
