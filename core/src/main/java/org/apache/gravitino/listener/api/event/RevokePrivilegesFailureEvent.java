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

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Represents an event triggered when an attempt to revoke privileges from a role fails due to an
 * exception.
 */
@DeveloperApi
public class RevokePrivilegesFailureEvent extends RoleFailureEvent {
  private final String roleName;
  private final MetadataObject metadataObject;
  private final Set<Privilege> privileges;

  /**
   * Constructs a new {@code RevokePrivilegesFailureEvent} instance.
   *
   * @param user the user who initiated the event
   * @param metalake the name of the metalake
   * @param exception the exception that occurred during the revocation attempt
   * @param roleName the name of the role from which privileges were attempted to be revoked
   * @param metadataObject the {@code MetadataObject} instance associated with the role
   * @param privileges the set of privileges intended to be revoked; if {@code null}, an empty set
   *     is used
   */
  public RevokePrivilegesFailureEvent(
      String user,
      String metalake,
      Exception exception,
      String roleName,
      MetadataObject metadataObject,
      Set<Privilege> privileges) {
    super(user, NameIdentifierUtil.ofRole(metalake, roleName), exception);
    this.roleName = roleName;
    this.metadataObject = metadataObject;
    this.privileges = privileges == null ? ImmutableSet.of() : ImmutableSet.copyOf(privileges);
  }

  /**
   * Returns the name of the role from which privileges were attempted to be revoked.
   *
   * @return the role name
   */
  public String roleName() {
    return roleName;
  }

  /**
   * Returns the associated {@code MetadataObject} instance.
   *
   * @return the {@code MetadataObject} instance linked to the role
   */
  public MetadataObject metadataObject() {
    return metadataObject;
  }

  /**
   * Returns the set of privileges intended to be revoked.
   *
   * @return an immutable set of privileges
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
