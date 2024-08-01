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

import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;

/** Interface for authorization Role plugin operation of the underlying access control system */
interface RoleAuthorizationPlugin {
  /**
   * After creating a role in Gravitino, this method is called to create the role in the underlying
   * system.<br>
   *
   * @param role The entity of the Role.
   * @return True if the create operation success; False if the create operation failed.
   * @throws RuntimeException If creating the Role encounters storage issues.
   */
  Boolean onRoleCreated(Role role) throws RuntimeException;

  /**
   * After acquiring a role from Gravitino, this method is called to acquire the role in the
   * underlying system.<br>
   * Because role information is already stored in the Gravitino, so we don't need to get the Role
   * from the underlying access control system. <br>
   * We only need to check if the Role exists in the underlying access control system.
   *
   * @param role The entity of the Role.
   * @return IF exist return true, else return false.
   * @throws RuntimeException If getting the Role encounters underlying access control system
   *     issues.
   */
  Boolean onRoleAcquired(Role role) throws RuntimeException;

  /**
   * After deleting a role from Gravitino, this method is called to delete the role in the
   * underlying system. <br>
   *
   * @param role The entity of the Role.
   * @return True if the Role was successfully deleted, false only when there's no such role
   * @throws RuntimeException If deleting the Role encounters storage issues.
   */
  Boolean onRoleDeleted(Role role) throws RuntimeException;

  /**
   * After updating a role in Gravitino, this method is called to update the role in the underlying
   * system. <br>
   *
   * @param role The entity of the Role.
   * @param changes role changes apply to the role.
   * @return True if the update operation is successful; False if the update operation fails.
   * @throws RuntimeException If update role encounters storage issues.
   */
  Boolean onRoleUpdated(Role role, RoleChange... changes) throws RuntimeException;
}
