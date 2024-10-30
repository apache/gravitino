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
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.exceptions.AuthorizationPluginException;

/** Interface for authorization Role plugin operation of the underlying access control system */
interface RoleAuthorizationPlugin {
  /**
   * After creating a role in Gravitino, this method is called to create the role in the underlying
   * system.<br>
   *
   * @param role The entity of the Role.
   * @return True if the create operation success; False if the create operation failed.
   * @throws AuthorizationPluginException If creating the Role encounters storage issues.
   */
  Boolean onRoleCreated(Role role) throws AuthorizationPluginException;

  /**
   * After acquiring a role from Gravitino, this method is called to acquire the role in the
   * underlying system.<br>
   * Because role information is already stored in the Gravitino, so we don't need to get the Role
   * from the underlying access control system. <br>
   * We only need to check if the Role exists in the underlying access control system.
   *
   * @param role The entity of the Role.
   * @return IF exist return true, else return false.
   * @throws AuthorizationPluginException If getting the Role encounters underlying access control
   *     system issues.
   */
  Boolean onRoleAcquired(Role role) throws AuthorizationPluginException;

  /**
   * After deleting a role from Gravitino, this method is called to delete the role in the
   * underlying system. <br>
   *
   * @param role The entity of the Role.
   * @return True if the Role was successfully deleted, false only when there's no such role
   * @throws AuthorizationPluginException If deleting the Role encounters storage issues.
   */
  Boolean onRoleDeleted(Role role) throws AuthorizationPluginException;

  /**
   * After updating a role in Gravitino, this method is called to update the role in the underlying
   * system. <br>
   *
   * @param role The entity of the Role.
   * @param changes role changes apply to the role.
   * @return True if the update operation is successful; False if the update operation fails.
   * @throws AuthorizationPluginException If update role encounters storage issues.
   */
  Boolean onRoleUpdated(Role role, RoleChange... changes) throws AuthorizationPluginException;

  /**
   * After granting roles to a user from Gravitino, this method is called to grant roles to the user
   * in the underlying system. <br>
   *
   * @param user The entity of the User.
   * @param roles The entities of the Roles.
   * @return True if the Grant was successful, false if the Grant was failed.
   * @throws AuthorizationPluginException If granting roles to a user encounters storage issues.
   */
  Boolean onGrantedRolesToUser(List<Role> roles, User user) throws AuthorizationPluginException;

  /**
   * After revoking roles from a user from Gravitino, this method is called to revoke roles from the
   * user in the underlying system. <br>
   *
   * @param user The entity of the User.
   * @param roles The entities of the Roles.
   * @return True if the revoke was successfully removed, false if the revoke failed.
   * @throws AuthorizationPluginException If revoking roles from a user encounters storage issues.
   */
  Boolean onRevokedRolesFromUser(List<Role> roles, User user) throws AuthorizationPluginException;

  /**
   * After granting roles to a group from Gravitino, this method is called to grant roles to the
   * group in the underlying system. <br>
   *
   * @param group The entity of the Group.
   * @param roles The entities of the Roles.
   * @return True if the revoke was successfully removed, False if the revoke failed.
   * @throws AuthorizationPluginException If granting roles to a group encounters storage issues.
   */
  Boolean onGrantedRolesToGroup(List<Role> roles, Group group) throws AuthorizationPluginException;

  /**
   * After revoking roles from a group from Gravitino, this method is called to revoke roles from
   * the group in the underlying system. <br>
   *
   * @param group The entity of the Group.
   * @param roles The entities of the Roles.
   * @return True if the revoke was successfully removed, False if the revoke failed.
   * @throws AuthorizationPluginException If revoking roles from a group encounters storage issues.
   */
  Boolean onRevokedRolesFromGroup(List<Role> roles, Group group)
      throws AuthorizationPluginException;
}
