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
import org.apache.gravitino.listener.api.info.RoleInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered after a role is successfully created. */
@DeveloperApi
public class CreateRoleEvent extends RoleEvent {
  private final RoleInfo createdRoleInfo;

  /**
   * Constructs a new {@code CreateRoleEvent} instance.
   *
   * @param initiator the user who initiated the event.
   * @param metalake the metalake name where the role was created.
   * @param createdRoleInfo the information of the created role.
   */
  public CreateRoleEvent(String initiator, String metalake, RoleInfo createdRoleInfo) {
    super(initiator, NameIdentifierUtil.ofRole(metalake, createdRoleInfo.roleName()));
    this.createdRoleInfo = createdRoleInfo;
  }

  /**
   * Returns the created role information.
   *
   * @return the {@code RoleInfo} instance containing details of the created role.
   */
  public RoleInfo createdRoleInfo() {
    return createdRoleInfo;
  }

  /**
   * Returns the operation type of this event.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.CREATE_ROLE;
  }
}
