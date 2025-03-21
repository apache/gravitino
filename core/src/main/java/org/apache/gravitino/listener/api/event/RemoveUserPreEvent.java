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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.info.UserInfo;

/** Represents an event triggered before remove a user from specific metalake. */
@DeveloperApi
public class RemoveUserPreEvent extends UserPreEvent {
  private final UserInfo removeUserRequest;

  /**
   * Construct a new {@link RemoveUserPreEvent} instance with the specified user and identifier.
   *
   * @param user the user who initiated the remove user operation.
   * @param identifier the identifier of the metalake where the user is removed.
   * @param removeUserRequest the user information to be removed from the metalake.
   */
  protected RemoveUserPreEvent(String user, NameIdentifier identifier, UserInfo removeUserRequest) {
    super(user, identifier);

    this.removeUserRequest = removeUserRequest;
  }

  /**
   * Returns the user information of the user which is to be removed from the metalake.
   *
   * @return the user information.
   */
  public UserInfo removeUserRequest() {
    return removeUserRequest;
  }

  /**
   * Returns the operation type of this event.
   *
   * @return The operation type of this event.
   */
  @Override
  public OperationType operationType() {
    return OperationType.REMOVE_USER;
  }
}
