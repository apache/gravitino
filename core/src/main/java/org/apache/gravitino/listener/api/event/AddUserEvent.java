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

/** Represents an event that is generated after a user is successfully added to the metalake. */
@DeveloperApi
public class AddUserEvent extends UserEvent {
  private UserInfo userInfo;

  /**
   * Construct a new {@link AddUserEvent} instance with the specified initiator, identifier, and
   * user information.
   *
   * @param initiator the user who initiated the add-user request.
   * @param identifier the identifier of the metalake which the user is added to.
   * @param userInfo the user information.
   */
  protected AddUserEvent(String initiator, NameIdentifier identifier, UserInfo userInfo) {
    super(initiator, identifier);

    this.userInfo = userInfo;
  }

  /**
   * Returns the user information of a user which was added to the metalake.
   *
   * @return the {@link UserInfo} instance.
   */
  public UserInfo userInfo() {
    return userInfo;
  }

  /**
   * Returns the operation type for this event.
   *
   * @return the operation type for this event.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ADD_USER;
  }
}
