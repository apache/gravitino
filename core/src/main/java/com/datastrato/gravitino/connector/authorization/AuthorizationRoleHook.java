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

import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.RoleChange;
import com.datastrato.gravitino.authorization.SecurableObject;
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
   * Check a Role if exist in the underlying access control system.
   *
   * @param role The name of the Role.
   * @return IF exist return true, else return false.
   * @throws RuntimeException If getting the Role encounters underlying access control system
   *     issues.
   */
  Boolean onCheckRole(String role) throws RuntimeException;

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
