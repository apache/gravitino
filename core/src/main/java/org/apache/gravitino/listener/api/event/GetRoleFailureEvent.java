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
 * Represents an event triggered when an attempt to retrieve a role from a metalake fails due to an
 * exception.
 */
@DeveloperApi
public class GetRoleFailureEvent extends RoleFailureEvent {
  private final String roleName;

  /**
   * Constructs a new {@code GetRoleFailureEvent} instance.
   *
   * @param user the user who initiated the retrieval attempt
   * @param metalake the metalake from which the role is being retrieved
   * @param exception the exception that occurred during role retrieval
   * @param roleName the name of the role being retrieved
   */
  public GetRoleFailureEvent(String user, String metalake, Exception exception, String roleName) {
    super(user, NameIdentifierUtil.ofRole(metalake, roleName), exception);

    this.roleName = roleName;
  }

  /**
   * Returns the name of the role that was attempted to be retrieved.
   *
   * @return the role name
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
