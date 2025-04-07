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

import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Represents an event triggered before retrieving a role from a specific metalake. This class
 * encapsulates the details of the role retrieval event prior to its execution.
 */
@DeveloperApi
public class GetRolePreEvent extends RolePreEvent {
  private final String roleName;

  /**
   * Constructs a new {@link GetRolePreEvent} instance with the specified initiator, identifier, and
   * role name.
   *
   * @param initiator The user who initiated the event.
   * @param metalake The name of the metalake where the role is being retrieved from.
   * @param roleName The name of the role being retrieved.
   */
  public GetRolePreEvent(String initiator, String metalake, String roleName) {
    super(initiator, NameIdentifierUtil.ofRole(metalake, roleName));
    this.roleName = roleName;
  }

  /**
   * Returns the name of the role.
   *
   * @return The name of the role being retrieved.
   */
  public String roleName() {
    return roleName;
  }

  /**
   * Returns the operation type of this event.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_ROLE;
  }
}
