/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import java.util.List;

/** Interface for authorization Role hooks operation of the underlying access control system */
public interface AuthorizationRoleHook {
  /**
   * Creates a new Role into underlying access control system.
   *
   * @param role The entity of the Role.
   * @param securableObjects The securable objects of the Role.
   * @return True if the create operation success; False if the create operation failed.
   * @throws RuntimeException If creating the Role encounters storage issues.
   */
  Boolean onCreateRole(Role role, List<SecurableObject> securableObjects) throws RuntimeException;

  /**
   * Gets a Role from underlying access control system.
   *
   * @param role The name of the Role.
   * @return The getting Role instance.
   * @throws RuntimeException If getting the Role encounters storage issues.
   */
  Role onGetRole(String role) throws RuntimeException;

  /**
   * Deletes a Role from underlying access control system.
   *
   * @param role The entity of the Role.
   * @return True if the Role was successfully deleted, false only when there's no such role
   * @throws RuntimeException If deleting the Role encounters storage issues.
   */
  Boolean onDeleteRole(Role role) throws RuntimeException;

  /**
   * Update a role into underlying access control system.
   *
   * @param role The entity of the Role.
   * @param changes role changes apply to the role.
   * @return True if the update operation success; False if the update operation failed.
   * @throws RuntimeException If update role encounters storage issues.
   */
  Boolean onUpdateRole(Role role, RoleChange... changes) throws RuntimeException;
}
