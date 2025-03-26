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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.authorization.Privilege;

/** Represents an event that is generated before revoking a set of privileges from a role. */
@DeveloperApi
public class RevokePrivilegesPreEvent extends RolePreEvent {
  private final String roleName;
  private final MetadataObject object;
  private final Set<Privilege> privileges;

  /**
   * Constructs a new {@link RevokePrivilegesPreEvent} instance with specified initiator,
   * identifier, role name, object, and privileges.
   *
   * @param initiator the user who initiated the event.
   * @param identifier the identifier of the metalake which is being operated on.
   * @param roleName the name of the role.
   * @param object the {@link MetadataObject} instance.
   * @param privileges the set of privileges to revoke.
   */
  public RevokePrivilegesPreEvent(
      String initiator,
      NameIdentifier identifier,
      String roleName,
      MetadataObject object,
      Set<Privilege> privileges) {
    super(initiator, identifier);
    this.roleName = roleName;
    this.object = object;
    this.privileges = privileges;
  }

  /**
   * Returns the name of the role.
   *
   * @return the name of the role.
   */
  public String roleName() {
    return roleName;
  }

  /**
   * Returns the MetadataObject instance.
   *
   * @return the {@link MetadataObject} instance.
   */
  public MetadataObject object() {
    return object;
  }

  /**
   * Returns the set of privileges to revoke.
   *
   * @return the set of privileges to revoke.
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
