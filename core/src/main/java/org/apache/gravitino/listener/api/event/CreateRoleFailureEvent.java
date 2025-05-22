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

/**
 * Represents an event triggered when an attempt to create a role in a metalake fails due to an
 * exception.
 */
@DeveloperApi
public class CreateRoleFailureEvent extends RoleFailureEvent {
  private final RoleInfo createRoleRequest;
  /**
   * Constructs a new {@code CreateRoleFailureEvent} instance.
   *
   * @param initiator the user who initiated the event.
   * @param metalake the target metalake context for the role creation
   * @param exception the exception that caused the failure
   * @param createRoleRequest the role information that failed to be created
   */
  public CreateRoleFailureEvent(
      String initiator, String metalake, Exception exception, RoleInfo createRoleRequest) {
    super(initiator, NameIdentifierUtil.ofRole(metalake, createRoleRequest.roleName()), exception);

    this.createRoleRequest = createRoleRequest;
  }

  /**
   * Returns the role information that failed to be created.
   *
   * @return The {@link RoleInfo} instance.
   */
  public RoleInfo createRoleRequest() {
    return createRoleRequest;
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
