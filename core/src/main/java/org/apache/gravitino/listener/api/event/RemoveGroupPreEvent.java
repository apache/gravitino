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

/** Represents an event triggered before remove a group from specific metalake. */
@DeveloperApi
public class RemoveGroupPreEvent extends GroupPreEvent {
  private final String groupName;

  /**
   * Construct a new {@link RemoveGroupPreEvent} instance with initiator, identifier and group name.
   *
   * @param initiator the user who initiated the remove group operation.
   * @param identifier the identifier of the metalake where the group is removed.
   * @param groupName the group name which is requested to be removed from the metalake.
   */
  protected RemoveGroupPreEvent(String initiator, NameIdentifier identifier, String groupName) {
    super(initiator, identifier);

    this.groupName = groupName;
  }

  /**
   * Returns the group information which is being removed from the metalake.
   *
   * @return the group name which is requested to be removed from the metalake.
   */
  public String groupName() {
    return groupName;
  }

  /**
   * Returns the operation type for this event.
   *
   * @return the operation type for this event.
   */
  @Override
  public OperationType operationType() {
    return OperationType.REMOVE_GROUP;
  }
}
