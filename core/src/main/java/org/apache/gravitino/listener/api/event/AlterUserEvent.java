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

import javax.annotation.Nullable;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.UserChange;
import org.apache.gravitino.listener.api.info.UserInfo;

/** Represents an event triggered upon the successful alteration of a user. */
@DeveloperApi
public final class AlterUserEvent extends UserEvent {
  private final UserInfo updatedUserInfo;
  private final UserChange[] userChanges;

  /**
   * Creates a new {@link AlterUserEvent}.
   *
   * @param initiator The user who initiated the request.
   * @param metalake The metalake name.
   * @param userChanges The changes applied to the user.
   * @param updatedUserInfo The post-alteration state of the user.
   */
  public AlterUserEvent(
      String initiator, String metalake, UserChange[] userChanges, UserInfo updatedUserInfo) {
    super(initiator, AuthorizationUtils.ofUserId(metalake, updatedUserInfo.id()));
    this.userChanges = userChanges != null ? userChanges.clone() : null;
    this.updatedUserInfo = updatedUserInfo;
  }

  /**
   * Returns the final state of the user after successful alteration.
   *
   * @return The updated user information.
   */
  public UserInfo updatedUserInfo() {
    return updatedUserInfo;
  }

  /**
   * Returns the specific changes that were made to the user.
   *
   * @return An array of {@link UserChange}, or null.
   */
  @Nullable
  public UserChange[] changes() {
    return userChanges;
  }

  @Override
  public OperationType operationType() {
    return OperationType.ALTER_USER;
  }
}
