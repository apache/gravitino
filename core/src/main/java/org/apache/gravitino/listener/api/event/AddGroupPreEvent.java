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

/** Represents an event triggered before a group is added to a metalake. */
@DeveloperApi
public class AddGroupPreEvent extends GroupPreEvent {
  private final String groupName;

  /**
   * Constructs a new {@link AddGroupPreEvent} with the specified initiator, metalake name, and
   * group name.
   *
   * @param initiator the user who initiated the add-group request.
   * @param metalake the name of the metalake where the group is to be added.
   * @param groupName the name of the group that is requested to be added to the metalake.
   */
  public AddGroupPreEvent(String initiator, String metalake, String groupName) {
    super(initiator, NameIdentifierUtil.ofGroup(metalake, groupName));

    this.groupName = groupName;
  }

  /**
   * Retrieves the name of the group that is being requested to be added to the metalake.
   *
   * @return the name of the group to be added.
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
