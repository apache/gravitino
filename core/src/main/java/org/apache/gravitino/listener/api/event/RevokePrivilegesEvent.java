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

import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.listener.api.info.RoleInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered after privileges are revoked from a role. */
@DeveloperApi
public class RevokePrivilegesEvent extends RoleEvent {
  private final RoleInfo revokedRoleInfo;
  private final MetadataObject object;
  private final Set<Privilege> privileges;

  /**
   * Constructs a new {@code RevokePrivilegesEvent} instance.
   *
   * @param initiator the user who initiated the event.
   * @param metalake the metalake name where the event occurred.
   * @param revokedRoleInfo the {@code RoleInfo} of the role from which privileges were revoked.
   * @param object the {@code MetadataObject} associated with the role.
   * @param privileges the set of privileges that were revoked.
   */
  public RevokePrivilegesEvent(
      String initiator,
      String metalake,
      RoleInfo revokedRoleInfo,
      MetadataObject object,
      Set<Privilege> privileges) {
    super(initiator, NameIdentifierUtil.ofRole(metalake, revokedRoleInfo.roleName()));

    this.revokedRoleInfo = revokedRoleInfo;
    this.object = object;
    this.privileges = privileges;
  }

  /**
   * Returns the role information of the role from which privileges were revoked.
   *
   * @return the {@code RoleInfo} instance.
   */
  public RoleInfo revokedRoleInfo() {
    return revokedRoleInfo;
  }

  /**
   * Returns the metadata object associated with the role.
   *
   * @return the {@code MetadataObject} instance.
   */
  public MetadataObject object() {
    return object;
  }

  /**
   * Returns the set of privileges that were revoked.
   *
   * @return a set of {@code Privilege} instances.
   */
  public Set<Privilege> privileges() {
    return privileges;
  }

  /**
   * Returns the operation type of this event.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.REVOKE_PRIVILEGES;
  }
}
