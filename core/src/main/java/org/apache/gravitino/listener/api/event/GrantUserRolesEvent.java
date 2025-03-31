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

/** Represents an event triggered after the operation of granting roles to a user. */
@DeveloperApi
public class GrantUserRolesEvent extends UserEvent {
  private final UserInfo grantUserInfo;
  private final List<String> roles;

  /**
   * Constructs a new {@link GrantUserRolesEvent} instance with the specified initiator, metalake
   * name, user information, and roles granted.
   *
   * @param initiator the user who initiated the role-granting operation.
   * @param metalake the name of the metalake that the operation affects.
   * @param grantUserInfo the user information of the user whose roles are being granted.
   * @param roles the list of roles that are granted to the user.
   */
  public GrantUserRolesEvent(
      String initiator, String metalake, UserInfo grantUserInfo, List<String> roles) {
    super(initiator, NameIdentifierUtil.ofUser(metalake, grantUserInfo.name()));

    this.grantUserInfo = grantUserInfo;
    this.roles = roles == null ? ImmutableList.of() : ImmutableList.copyOf(roles);
  }

  /**
   * Returns the user information of the user to whom the roles are granted.
   *
   * @return the {@link UserInfo} instance containing the details of the user.
   */
  public UserInfo grantUserInfo() {
    return grantUserInfo;
  }

  /**
   * Returns the list of roles that have been granted to the user.
   *
   * @return the list of roles granted to the user.
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
    return OperationType.GRANT_USER_ROLES;
  }
}
