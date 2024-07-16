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
package org.apache.gravitino.connector.authorization;

import java.util.List;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.User;

/**
 * Interface for authorization User and Group plugin operation of the underlying access control
 * system.
 */
interface UserGroupAuthorizationPlugin {
  /**
   * After adding a User to Gravitino, this method is called to add the User to the underlying
   * system. <br>
   *
   * @param user The user entity.
   * @return True if the add User was successfully added, false if the add User failed.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  Boolean onUserAdded(User user) throws RuntimeException;

  /**
   * After removing a User from Gravitino, this method is called to remove the User from the
   * underlying system. <br>
   *
   * @param user The user entity.
   * @return True if the User was successfully removed, false if the remove User failed.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  Boolean onUserRemoved(User user) throws RuntimeException;

  /**
   * After acquiring a User from Gravitino, this method is called to acquire the User in the
   * underlying system. <br>
   * Because User information is already stored in the Gravitino, so we don't need to get the User
   * from the underlying access control system. <br>
   * We only need to check if the User exists in the underlying access control system.
   *
   * @param user The user entity.
   * @return IF exist return true, else return false.
   * @throws RuntimeException If getting the User encounters underlying access control system
   *     issues.
   */
  Boolean onUserAcquired(User user) throws RuntimeException;

  /**
   * After adding a Group to Gravitino, this method is called to add the Group to the underlying
   * system. <br>
   *
   * @param group The group entity.
   * @return True if the add Group was successfully added, false if the add Group failed.
   * @throws RuntimeException If adding the Group encounters storage issues.
   */
  Boolean onGroupAdded(Group group) throws RuntimeException;

  /**
   * After removing a Group from Gravitino, this method is called to remove the Group from the
   * underlying system. <br>
   *
   * @param group The group entity.
   * @return True if the remove Group was successfully removed, false if the remove Group was
   *     failed.
   * @throws RuntimeException If removing the Group encounters storage issues.
   */
  Boolean onGroupRemoved(Group group) throws RuntimeException;

  /**
   * After acquiring a Group from Gravitino, this method is called to acquire the Group in the
   * underlying system. <br>
   * Because Group information is already stored in the Gravitino, so we don't need to get the Group
   * from the underlying access control system. <br>
   * We only need to check if the Group exists in the underlying access control system. <br>
   *
   * @param group The group entity.
   * @return If exist return true, else return false.
   * @throws RuntimeException If getting the Group encounters underlying access control system
   *     issues.
   */
  Boolean onGroupAcquired(Group group) throws RuntimeException;

  /**
   * After granting roles to a user from Gravitino, this method is called to grant roles to the user
   * in the underlying system. <br>
   *
   * @param user The entity of the User.
   * @param roles The entities of the Roles.
   * @return True if the Grant was successful, false if the Grant was failed.
   * @throws RuntimeException If granting roles to a user encounters storage issues.
   */
  Boolean onGrantedRolesToUser(List<Role> roles, User user) throws RuntimeException;

  /**
   * After revoking roles from a user from Gravitino, this method is called to revoke roles from the
   * user in the underlying system. <br>
   *
   * @param user The entity of the User.
   * @param roles The entities of the Roles.
   * @return True if the revoke was successfully removed, false if the revoke failed.
   * @throws RuntimeException If revoking roles from a user encounters storage issues.
   */
  Boolean onRevokedRolesFromUser(List<Role> roles, User user) throws RuntimeException;

  /**
   * After granting roles to a group from Gravitino, this method is called to grant roles to the
   * group in the underlying system. <br>
   *
   * @param group The entity of the Group.
   * @param roles The entities of the Roles.
   * @return True if the revoke was successfully removed, False if the revoke failed.
   * @throws RuntimeException If granting roles to a group encounters storage issues.
   */
  Boolean onGrantedRolesToGroup(List<Role> roles, Group group) throws RuntimeException;

  /**
   * After revoking roles from a group from Gravitino, this method is called to revoke roles from
   * the group in the underlying system. <br>
   *
   * @param group The entity of the Group.
   * @param roles The entities of the Roles.
   * @return True if the revoke was successfully removed, False if the revoke failed.
   * @throws RuntimeException If revoking roles from a group encounters storage issues.
   */
  Boolean onRevokedRolesFromGroup(List<Role> roles, Group group) throws RuntimeException;
}
