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

package org.apache.gravitino.listener.api.event;

import java.util.List;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.listener.api.info.RoleInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered after privileges are overridden in a role. */
@DeveloperApi
public class OverridePrivilegesEvent extends RoleEvent {
  private final RoleInfo updatedRoleInfo;
  private final List<SecurableObject> securableObjectsToOverride;

  /**
   * Constructs a new {@code OverridePrivilegesEvent} instance.
   *
   * @param initiator the user who initiated the event.
   * @param metalake the metalake name where the event occurred.
   * @param updatedRoleInfo the {@code RoleInfo} of the role that was updated with overridden
   *     privileges.
   * @param securableObjectsToOverride the list of securable objects that were overridden in the
   *     role.
   */
  public OverridePrivilegesEvent(
      String initiator,
      String metalake,
      RoleInfo updatedRoleInfo,
      List<SecurableObject> securableObjectsToOverride) {
    super(initiator, NameIdentifierUtil.ofRole(metalake, updatedRoleInfo.roleName()));

    this.updatedRoleInfo = updatedRoleInfo;
    this.securableObjectsToOverride = securableObjectsToOverride;
  }

  /**
   * Returns the role information of the role that was updated.
   *
   * @return the {@code RoleInfo} instance containing details of the updated role.
   */
  public RoleInfo updatedRoleInfo() {
    return updatedRoleInfo;
  }

  /**
   * Returns the list of securable objects that were overridden.
   *
   * @return a list of {@code SecurableObject} instances.
   */
  public List<SecurableObject> securableObjectsToOverride() {
    return securableObjectsToOverride;
  }

  /**
   * Returns the operation type of this event.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.OVERRIDE_PRIVILEGES;
  }
}
