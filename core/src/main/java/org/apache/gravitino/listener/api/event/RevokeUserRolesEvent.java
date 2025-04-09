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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.info.UserInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered after the operation of revoking roles from a user. */
@DeveloperApi
public class RevokeUserRolesEvent extends UserEvent {
  private final UserInfo revokedUserInfo;
  private final List<String> roles;

  /**
   * Constructs a new {@link RevokeUserRolesEvent} instance with the specified initiator, metalake
   * name, user information, and revoked roles.
   *
   * @param initiator the user who initiated the role-revocation operation.
   * @param metalake the name of the metalake that the operation affects.
   * @param revokedUserInfo the user information of the user whose roles are being revoked.
   * @param roles the list of roles that have been revoked from the user.
   */
  public RevokeUserRolesEvent(
      String initiator, String metalake, UserInfo revokedUserInfo, List<String> roles) {
    super(initiator, NameIdentifierUtil.ofUser(metalake, revokedUserInfo.name()));

    this.revokedUserInfo = revokedUserInfo;
    this.roles = roles == null ? ImmutableList.of() : ImmutableList.copyOf(roles);
  }

  /**
   * Returns the user information of the user whose roles have been revoked.
   *
   * @return the {@link UserInfo} instance containing the details of the user.
   */
  public UserInfo revokedUserInfo() {
    return revokedUserInfo;
  }

  /**
   * Returns the list of roles that have been revoked from the user.
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
