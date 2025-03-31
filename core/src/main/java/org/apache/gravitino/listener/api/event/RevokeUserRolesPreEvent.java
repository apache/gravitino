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
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered before revoking roles from a user. */
@DeveloperApi
public class RevokeUserRolesPreEvent extends UserPreEvent {
  private final String userName;
  private final List<String> roles;

  /**
   * Constructs a new {@link RevokeUserRolesPreEvent} instance with the specified initiator,
   * metalake name, username, and roles to be revoked.
   *
   * @param initiator the user who initiated the event to revoke roles.
   * @param metalake the name of the metalake on which the operation is being performed.
   * @param userName the username of the user whose roles are being revoked.
   * @param roles the list of roles to be revoked from the user.
   */
  public RevokeUserRolesPreEvent(
      String initiator, String metalake, String userName, List<String> roles) {
    super(initiator, NameIdentifierUtil.ofUser(metalake, userName));

    this.userName = userName;
    this.roles = roles;
  }

  /**
   * Returns the username of the user whose roles are being revoked.
   *
   * @return the username of the user.
   */
  public String userName() {
    return userName;
  }

  /**
   * Returns the list of roles that are being revoked from the user.
   *
   * @return the list of revoked roles.
   */
  public List<String> roles() {
    return roles;
  }

  /**
   * Returns the operation type of this event.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.REVOKE_USER_ROLES;
  }
}
