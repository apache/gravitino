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

/** Represents an event that is triggered after a group is successfully removed from a metalake. */
@DeveloperApi
public class RemoveGroupEvent extends GroupEvent {
  private final String removedGroupName;
  private final boolean isExists;

  /**
   * Constructs a new {@link RemoveGroupEvent} with the specified initiator, metalake name, removed
   * group name, and existence status.
   *
   * @param initiator the user who initiated the remove group operation.
   * @param metalake the name of the metalake from which the group was removed.
   * @param removedGroupName the name of the group that was removed.
   * @param isExists {@code true} if the group was successfully removed, {@code false} if there was
   *     no such group in the metalake.
   */
  public RemoveGroupEvent(
      String initiator, String metalake, String removedGroupName, boolean isExists) {
    super(initiator, NameIdentifierUtil.ofGroup(metalake, removedGroupName));

    this.removedGroupName = removedGroupName;
    this.isExists = isExists;
  }

  /**
   * Returns the name of the group that was removed.
   *
   * @return the name of the removed group.
   */
  public String removedGroupName() {
    return removedGroupName;
  }

  /**
   * Returns whether the group was removed from the metalake.
   *
   * @return {@code true} if the group was removed, {@code false} if no such group exists in the
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
    return OperationType.REMOVE_GROUP;
  }
}
