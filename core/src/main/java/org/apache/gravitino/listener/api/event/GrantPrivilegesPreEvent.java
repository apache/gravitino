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
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Represents an event generated before granting a set of privileges to a role. This class
 * encapsulates the details of the privilege granting event prior to its execution.
 */
@DeveloperApi
public class GrantPrivilegesPreEvent extends RolePreEvent {
  private final String roleName;
  private final MetadataObject metadataObject;
  private final Set<Privilege> privileges;

  /**
   * Constructs a new {@link GrantPrivilegesPreEvent} instance with the specified initiator,
   * identifier, role name, object, and privileges.
   *
   * @param initiator The name of the user who initiated the event.
   * @param metalake The name of the metalake.
   * @param roleName The name of the role to which privileges will be granted.
   * @param metadataObject The {@link MetadataObject} instance related to the role.
   * @param privileges The set of privileges to grant to the role.
   */
  public GrantPrivilegesPreEvent(
      String initiator,
      String metalake,
      String roleName,
      MetadataObject metadataObject,
      Set<Privilege> privileges) {
    super(initiator, NameIdentifierUtil.ofRole(metalake, roleName));

    this.roleName = roleName;
    this.metadataObject = metadataObject;
    this.privileges = privileges;
  }

  /**
   * Returns the name of the role.
   *
   * @return The name of the role to which privileges will be granted.
   */
  public String roleName() {
    return roleName;
  }

  /**
   * Returns the {@link MetadataObject} instance.
   *
   * @return The {@link MetadataObject} instance related to the role.
   */
  public MetadataObject object() {
    return metadataObject;
  }

  /**
   * Returns the set of privileges to grant.
   *
   * @return The set of privileges to grant to the role.
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
