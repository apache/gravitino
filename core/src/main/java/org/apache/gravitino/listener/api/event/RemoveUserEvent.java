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

/** Represents an event that is generated after a user is successfully removed from the metalake. */
@DeveloperApi
public class RemoveUserEvent extends UserEvent {
  private final UserInfo userInfo;
  private final boolean isExists;

  /**
   * Construct a new {@link RemoveUserEvent} instance with the specified initiator, identifier, user
   * info and is removed flag.
   *
   * @param initiator the user who initiated the remove user operation.
   * @param identifier the identifier of the metalake where the user is removed.
   * @param userInfo the user information.
   * @param isExists {@code true} if metalake successfully remove the user, {@code false} only when
   *     there's no such user.
   */
  protected RemoveUserEvent(
      String initiator, NameIdentifier identifier, UserInfo userInfo, boolean isExists) {
    super(initiator, identifier);

    this.userInfo = userInfo;
    this.isExists = isExists;
  }

  /**
   * Returns the user information for the user which was removed from the metalake.
   *
   * @return the {@link UserInfo} instance.
   */
  public UserInfo userInfo() {
    return userInfo;
  }

  /**
   * Returns whether the user was removed from the metalake.
   *
   * @return {@code true} if the user was removed, {@code false} representing there is no such user
   *     in the metalake.
   */
  public boolean isExists() {
    return isExists;
  }

  /**
   * Returns the operation type of this event.
   *
   * @return the operation type of this event.
   */
  @Override
  public OperationType operationType() {
    return OperationType.REMOVE_USER;
  }
}
