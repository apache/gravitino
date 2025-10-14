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

/** Represents an event triggered after a user is successfully removed from a metalake. */
@DeveloperApi
public class RemoveUserEvent extends UserEvent {
  private final String removedUserName;
  private final boolean isExists;

  /**
   * Constructs a new {@link RemoveUserEvent} instance with the specified initiator, metalake name,
   * removed username, and removal status.
   *
   * @param initiator the user who initiated the remove user operation.
   * @param metalake the name of the metalake from which the user was removed.
   * @param removedUserName the username of the user that was removed.
   * @param isExists {@code true} if the user was successfully removed, {@code false} if no such
   *     user exists in the metalake.
   */
  public RemoveUserEvent(
      String initiator, String metalake, String removedUserName, boolean isExists) {
    super(initiator, NameIdentifierUtil.ofUser(metalake, removedUserName));

    this.removedUserName = removedUserName;
    this.isExists = isExists;
  }

  /**
   * Returns the username of the user that was removed from the metalake.
   *
   * @return the username of the removed user.
   */
  public String removedUserName() {
    return removedUserName;
  }

  /**
   * Returns whether the user was removed from the metalake.
   *
   * @return {@code true} if the user was removed, {@code false} if no such user exists in the
   *     metalake.
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
