/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

/** Authorization Role interface. */
public interface AuthorizationRole {
  /**
   * Create a role.
   *
   * @param name The role's name.
   * @return The created role object.
   * @throws UnsupportedOperationException If the authorization does not support to create a role.
   */
  Role createRole(String name) throws UnsupportedOperationException;

  /**
   * Drop a role.
   *
   * @param role to be deleted.
   * @return True if the delete operation success; False otherwise.
   * @throws UnsupportedOperationException If the authorization does not support to drop a role.
   */
  Boolean dropRole(Role role) throws UnsupportedOperationException;

  /**
   * Grant a role to a user.
   *
   * @param roleName role name.
   * @param userName user name.
   * @return True if the grant operation success; False otherwise.
   * @throws UnsupportedOperationException If the authorization does not support to grant a role to
   *     a user.
   */
  Boolean toUser(String roleName, String userName) throws UnsupportedOperationException;

  /**
   * Grant a role to a group.
   *
   * @param roleName role name.
   * @param groupName group name.
   * @return True if the grant operation success; False otherwise.
   * @throws UnsupportedOperationException If the authorization does not support to grant a role to
   *     a group.
   */
  Boolean toGroup(String roleName, String groupName) throws UnsupportedOperationException;

  /**
   * Update a role.
   *
   * @param roleName role name.
   * @param changes role changes apply to the role.
   * @return True if the grant operation success; False otherwise.
   * @throws UnsupportedOperationException If the authorization does not support to update a role.
   */
  Role updateRole(String roleName, RoleChange... changes) throws UnsupportedOperationException;
}
