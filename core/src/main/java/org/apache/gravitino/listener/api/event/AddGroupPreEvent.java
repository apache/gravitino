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

/** Represents an event triggered before add a group to a metalake. */
@DeveloperApi
public class AddGroupPreEvent extends GroupPreEvent {
  private final String groupName;

  /**
   * Construct a new {@link AddGroupPreEvent} instance with the given initiator, identifier and
   * group name.
   *
   * @param initiator the user who initiated the add-group request.
   * @param identifier the identifier of the metalake which the group is being added to.
   * @param groupName the group name which is requested to be added to the metalake.
   */
  protected AddGroupPreEvent(String initiator, NameIdentifier identifier, String groupName) {
    super(initiator, identifier);

    this.groupName = groupName;
  }

  /**
   * Returns the group information which is being added to the metalake.
   *
   * @return the group name which is requested to be added to the metalake.
   */
  public String groupName() {
    return groupName;
  }

  /**
   * Returns the operation type of this event.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ADD_GROUP;
  }
}
