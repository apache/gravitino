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
import org.apache.gravitino.listener.api.info.UserInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event generated after a user is successfully added to a metalake. */
@DeveloperApi
public class AddUserEvent extends UserEvent {
  private UserInfo addedUserInfo;

  /**
   * Constructs a new {@link AddUserEvent} instance with the specified initiator, metalake name, and
   * user information.
   *
   * @param initiator the user who initiated the request to add a user.
   * @param metalake the name of the metalake where the user was added.
   * @param addedUserInfo the user information of the newly added user.
   */
  public AddUserEvent(String initiator, String metalake, UserInfo addedUserInfo) {
    super(initiator, NameIdentifierUtil.ofUser(metalake, addedUserInfo.name()));

    this.addedUserInfo = addedUserInfo;
  }

  /**
   * Returns the user information of the user added to the metalake.
   *
   * @return the {@link UserInfo} instance containing the details of the added user.
   */
  public UserInfo addedUserInfo() {
    return addedUserInfo;
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
