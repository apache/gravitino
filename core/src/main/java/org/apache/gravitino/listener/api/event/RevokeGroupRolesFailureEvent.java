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

/**
 * Represents an event triggered when an attempt to revoke roles from a group fails due to an
 * exception.
 */
@DeveloperApi
public class RevokeGroupRolesFailureEvent extends GroupFailureEvent {
  private final String groupName;
  private final List<String> roles;

  /**
   * Creates a new instance of {@code RevokeGroupRolesFailureEvent}.
   *
   * @param initiator the user who initiated the operation
   * @param metalake the name of the metalake where the operation was attempted
   * @param exception the exception encountered during the operation
   * @param groupName the name of the group from which the roles were attempted to be revoked
   * @param roles the list of roles that were attempted to be revoked from the group
   */
  public RevokeGroupRolesFailureEvent(
      String initiator,
      String metalake,
      Exception exception,
      String groupName,
      List<String> roles) {
    super(initiator, NameIdentifierUtil.ofGroup(metalake, groupName), exception);

    this.groupName = groupName;
    this.roles = roles;
  }

  /**
   * Retrieves the name of the group from which the roles were attempted to be revoked.
   *
   * @return the group name involved in the failure event
   */
  public String groupName() {
    return groupName;
  }

  /**
   * Retrieves the list of roles that were attempted to be revoked from the group.
   *
   * @return the list of roles involved in the failure event
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
