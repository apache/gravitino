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

/** Represents an event triggered after successful get a user from specific metalake */
@DeveloperApi
public class GetUserEvent extends UserEvent {
  private final UserInfo getUserInfo;

  /**
   * Construct a new {@link GetUserEvent} instance with the given initiator, identifier and user
   * info.
   *
   * @param initiator the user who initiated the user-get request.
   * @param identifier the identifier of the metalake which the user is retrieved from.
   * @param getUserInfo the user information.
   */
  protected GetUserEvent(String initiator, NameIdentifier identifier, UserInfo getUserInfo) {
    super(initiator, identifier);

    this.getUserInfo = getUserInfo;
  }

  /**
   * Returns the user information for a user which is successfully retrieved from the metalake.
   *
   * @return the {@link UserInfo} instance.
   */
  public UserInfo getUserInfo() {
    return getUserInfo;
  }

  /**
   * Returns the operation type for this event.
   *
   * @return the operation type for this event.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_USER;
  }
}
