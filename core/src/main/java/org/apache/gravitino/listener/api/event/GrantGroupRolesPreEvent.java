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

/** Represents an event triggered before roles are granted to a group. */
@DeveloperApi
public class GrantGroupRolesPreEvent extends GroupPreEvent {
  private final String groupName;
  private final List<String> roles;

  /**
   * Constructs a new {@link GrantGroupRolesPreEvent} with the specified initiator, metalake name,
   * group name, and roles to be granted.
   *
   * @param initiator the user who initiated the role-granting event.
   * @param metalake the name of the metalake where the operation will be performed.
   * @param groupName the name of the group to which roles will be granted.
   * @param roles the list of roles to be granted to the group.
   */
  public GrantGroupRolesPreEvent(
      String initiator, String metalake, String groupName, List<String> roles) {
    super(initiator, NameIdentifierUtil.ofGroup(metalake, groupName));

    this.groupName = groupName;
    this.roles = roles;
  }

  /**
   * Retrieves the name of the group to which roles will be granted.
   *
   * @return the name of the group.
   */
  public String groupName() {
    return groupName;
  }

  /**
   * Returns the list of roles that are being granted to the group.
   *
   * @return the list of roles to be granted.
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
