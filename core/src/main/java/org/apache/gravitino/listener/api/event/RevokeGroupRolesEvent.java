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
import org.apache.gravitino.listener.api.info.GroupInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered after the operation of revoking roles from a group. */
@DeveloperApi
public class RevokeGroupRolesEvent extends GroupEvent {
  private final GroupInfo revokedGroupInfo;
  private final List<String> roles;

  /**
   * Constructs a new {@link RevokeGroupRolesEvent} with the specified initiator, metalake name,
   * revoked group information, and the list of revoked roles.
   *
   * @param initiator the user who initiated the role-revocation operation.
   * @param metalake the name of the metalake that the operation affects.
   * @param revokedGroupInfo the group information of the group whose roles are being revoked.
   * @param roles the list of roles that have been revoked from the group.
   */
  public RevokeGroupRolesEvent(
      String initiator, String metalake, GroupInfo revokedGroupInfo, List<String> roles) {
    super(initiator, NameIdentifierUtil.ofGroup(metalake, revokedGroupInfo.name()));

    this.revokedGroupInfo = revokedGroupInfo;
    this.roles = roles == null ? null : ImmutableList.copyOf(roles);
  }

  /**
   * Returns the group information of the group whose roles have been revoked.
   *
   * @return the {@link GroupInfo} instance containing the details of the group.
   */
  public GroupInfo revokedGroupInfo() {
    return revokedGroupInfo;
  }

  /**
   * Returns the list of roles that have been revoked from the group.
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
    return OperationType.REVOKE_GROUP_ROLES;
  }
}
