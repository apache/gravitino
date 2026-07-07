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
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.listener.api.info.UserInfo;

/** Represents an event triggered after successfully enabling a user by external id. */
@DeveloperApi
public class EnableUserEvent extends UserEvent {
  private final UserInfo updatedUserInfo;

  /**
   * Creates a new {EnableUserEvent}.
   *
   * @param initiator The user who initiated the request.
   * @param metalake The metalake name.
   * @param updatedUserInfo The updated user information.
   */
  public EnableUserEvent(String initiator, String metalake, UserInfo updatedUserInfo) {
    super(initiator, AuthorizationUtils.ofUserExternalId(metalake, updatedUserInfo.externalId()));
    this.updatedUserInfo = updatedUserInfo;
  }

  /**
   * Returns the updated user information.
   *
   * @return The user information.
   */
  public UserInfo updatedUserInfo() {
    return updatedUserInfo;
  }

  @Override
  public OperationType operationType() {
    return OperationType.ENABLE_USER;
  }
}
