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

/** Represents an event triggered after privileges are granted to a role. */
@DeveloperApi
public class GrantPrivilegesEvent extends RoleEvent {
  private final RoleInfo grantedRoleInfo;
  private final MetadataObject object;
  private final Set<Privilege> privileges;

  /**
   * Constructs a new {@code GrantPrivilegesEvent} instance.
   *
   * @param initiator the user who initiated the event.
   * @param metalake the metalake name where the event occurred.
   * @param grantedRoleInfo the {@code RoleInfo} of the role that was granted privileges.
   * @param privileges the set of privileges granted to the role.
   * @param object the {@code MetadataObject} associated with the role.
   */
  public GrantPrivilegesEvent(
      String initiator,
      String metalake,
      RoleInfo grantedRoleInfo,
      Set<Privilege> privileges,
      MetadataObject object) {
    super(initiator, NameIdentifierUtil.ofRole(metalake, grantedRoleInfo.roleName()));

    this.grantedRoleInfo = grantedRoleInfo;
    this.privileges = privileges;
    this.object = object;
  }

  /**
   * Returns the role information of the role that was granted privileges.
   *
   * @return the {@code RoleInfo} instance containing details of the granted role.
   */
  public RoleInfo grantedRoleInfo() {
    return grantedRoleInfo;
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
   * Returns the set of privileges granted to the role.
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
    return OperationType.GRANT_PRIVILEGES;
  }
}
