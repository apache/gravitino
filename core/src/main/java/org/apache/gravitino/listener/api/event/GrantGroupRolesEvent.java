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

/** Represents an event triggered after roles are granted to a group. */
@DeveloperApi
public class GrantGroupRolesEvent extends GroupEvent {
  private final GroupInfo grantedGroupInfo;
  private final List<String> roles;

  /**
   * Constructs a new {@link GrantGroupRolesEvent} with the specified initiator, metalake name,
   * group information, and granted roles.
   *
   * @param initiator the user who initiated the role-granting operation.
   * @param metalake the name of the metalake where the operation takes place.
   * @param grantedGroupInfo the group information of the group to which roles are granted.
   * @param roles the list of roles granted to the group.
   */
  public GrantGroupRolesEvent(
      String initiator, String metalake, GroupInfo grantedGroupInfo, List<String> roles) {
    super(initiator, NameIdentifierUtil.ofGroup(metalake, grantedGroupInfo.name()));
    this.grantedGroupInfo = grantedGroupInfo;
    this.roles = roles == null ? null : ImmutableList.copyOf(roles);
  }

  /**
   * Retrieves the {@link GroupInfo} of the group to which roles have been granted.
   *
   * @return the {@link GroupInfo} instance containing the details of the granted group.
   */
  public GroupInfo grantedGroupInfo() {
    return grantedGroupInfo;
  }

  /**
   * Returns the list of roles that have been granted to the group.
   *
   * @return the list of roles granted to the group.
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
    return OperationType.GRANT_GROUP_ROLES;
  }
}
