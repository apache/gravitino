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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.IllegalRoleException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.RoleAlreadyExistsException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;

/**
 * This interface is related to the access control. This interface is mainly used for
 * LifecycleHooks. The lifecycleHooks used the InvocationHandler. The InvocationHandler can only
 * hook the interfaces.
 */
public interface AccessControlDispatcher {
  /**
   * Adds a new User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return The added User instance.
   * @throws UserAlreadyExistsException If a User with the same name already exists.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  User addUser(String metalake, String user)
      throws UserAlreadyExistsException, NoSuchMetalakeException;

  /**
   * Removes a User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return True if the User was successfully removed, false only when there's no such user,
   *     otherwise it will throw an exception.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  boolean removeUser(String metalake, String user) throws NoSuchMetalakeException;

  /**
   * Gets a User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return The getting User instance.
   * @throws NoSuchUserException If the User with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If getting the User encounters storage issues.
   */
  User getUser(String metalake, String user) throws NoSuchUserException, NoSuchMetalakeException;

  /**
   * Lists the users.
   *
   * @param metalake The Metalake of the User.
   * @return The User list.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   */
  User[] listUsers(String metalake) throws NoSuchMetalakeException;

  /**
   * Lists the usernames.
   *
   * @param metalake The Metalake of the User.
   * @return The username list.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   */
  String[] listUserNames(String metalake) throws NoSuchMetalakeException;

  /**
   * Adds a new Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group The name of the Group.
   * @return The Added Group instance.
   * @throws GroupAlreadyExistsException If a Group with the same name already exists.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If adding the Group encounters storage issues.
   */
  Group addGroup(String metalake, String group)
      throws GroupAlreadyExistsException, NoSuchMetalakeException;

  /**
   * Removes a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @return True if the Group was successfully removed, false only when there's no such group,
   *     otherwise it will throw an exception.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If removing the Group encounters storage issues.
   */
  boolean removeGroup(String metalake, String group) throws NoSuchMetalakeException;

  /**
   * Gets a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group The name of the Group.
   * @return The getting Group instance.
   * @throws NoSuchGroupException If the Group with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If getting the Group encounters storage issues.
   */
  Group getGroup(String metalake, String group)
      throws NoSuchGroupException, NoSuchMetalakeException;

  /**
   * List groups
   *
   * @param metalake The Metalake of the Group.
   * @return The list of groups
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   */
  Group[] listGroups(String metalake);

  /**
   * List group names
   *
   * @param metalake The Metalake of the Group.
   * @return The list of group names
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   */
  String[] listGroupNames(String metalake);

  /**
   * Grant roles to a user.
   *
   * @param metalake The metalake of the User.
   * @param user The name of the User.
   * @param roles The names of the Role.
   * @return The User after granted.
   * @throws NoSuchUserException If the User with the given name does not exist.
   * @throws IllegalRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If granting roles to a user encounters storage issues.
   */
  User grantRolesToUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, IllegalRoleException, NoSuchMetalakeException;

  /**
   * Grant roles to a group.
   *
   * @param metalake The metalake of the Group.
   * @param group The name of the Group.
   * @param roles The names of the Role.
   * @return The Group after granted.
   * @throws NoSuchGroupException If the Group with the given name does not exist.
   * @throws IllegalRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If granting roles to a group encounters storage issues.
   */
  Group grantRolesToGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, IllegalRoleException, NoSuchMetalakeException;

  /**
   * Revoke roles from a group.
   *
   * @param metalake The metalake of the Group.
   * @param group The name of the Group.
   * @param roles The name of the Role.
   * @return The Group after revoked.
   * @throws NoSuchGroupException If the Group with the given name does not exist.
   * @throws IllegalRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If revoking roles from a group encounters storage issues.
   */
  Group revokeRolesFromGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, IllegalRoleException, NoSuchMetalakeException;

  /**
   * Revoke roles from a user.
   *
   * @param metalake The metalake of the User.
   * @param user The name of the User.
   * @param roles The name of the Role.
   * @return The User after revoked.
   * @throws NoSuchUserException If the User with the given name does not exist.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If revoking roles from a user encounters storage issues.
   */
  User revokeRolesFromUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, IllegalRoleException, NoSuchMetalakeException;

  /**
   * Judges whether the user is the service admin.
   *
   * @param user the name of the user
   * @return True if the user is service admin, otherwise false.
   */
  boolean isServiceAdmin(String user);

  /**
   * Creates a new Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @param properties The properties of the Role.
   * @param securableObjects The securable objects of the Role.
   * @return The created Role instance.
   * @throws RoleAlreadyExistsException If a Role with the same name already exists.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If creating the Role encounters storage issues.
   */
  Role createRole(
      String metalake,
      String role,
      Map<String, String> properties,
      List<SecurableObject> securableObjects)
      throws RoleAlreadyExistsException, NoSuchMetalakeException;

  /**
   * Gets a Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @return The getting Role instance.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If getting the Role encounters storage issues.
   */
  Role getRole(String metalake, String role) throws NoSuchRoleException, NoSuchMetalakeException;

  /**
   * Deletes a Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @return True if the Role was successfully deleted, false only when there's no such role,
   *     otherwise it will throw an exception.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If deleting the Role encounters storage issues.
   */
  boolean deleteRole(String metalake, String role) throws NoSuchMetalakeException;

  /**
   * Lists the role names.
   *
   * @param metalake The Metalake of the Role.
   * @return The role name list.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   */
  String[] listRoleNames(String metalake) throws NoSuchMetalakeException;

  /**
   * Lists the role names associated the metadata object.
   *
   * @param metalake The Metalake of the Role.
   * @param object The object of the Roles.
   * @return The role list.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws NoSuchMetadataObjectException If the Metadata object with the given name does not
   *     exist.
   */
  String[] listRoleNamesByObject(String metalake, MetadataObject object)
      throws NoSuchMetalakeException, NoSuchMetadataObjectException;

  /**
   * Grant privileges to a role.
   *
   * @param metalake The name of the Metalake.
   * @param role The name of the role.
   * @param privileges The privileges to grant.
   * @param object The object is associated with privileges to grant.
   * @return The role after granted.
   * @throws NoSuchRoleException If the role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If granting roles to a role encounters storage issues.
   */
  Role grantPrivilegeToRole(
      String metalake, String role, MetadataObject object, Set<Privilege> privileges)
      throws NoSuchGroupException, NoSuchRoleException;

  /**
   * Revoke privileges from a role.
   *
   * @param metalake The name if the Metalake.
   * @param role The name of the role.
   * @param privileges The privileges to revoke.
   * @param object The object is associated with privileges to revoke.
   * @return The role after revoked.
   * @throws NoSuchRoleException If the role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If revoking privileges from a role encounters storage issues.
   */
  Role revokePrivilegesFromRole(
      String metalake, String role, MetadataObject object, Set<Privilege> privileges)
      throws NoSuchMetalakeException, NoSuchRoleException;
}
