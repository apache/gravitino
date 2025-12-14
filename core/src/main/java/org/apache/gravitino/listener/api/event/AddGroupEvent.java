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
import org.apache.gravitino.listener.api.info.GroupInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered when a group is successfully added to a metalake. */
@DeveloperApi
public class AddGroupEvent extends GroupEvent {
  private final GroupInfo addedGroupInfo;

  /**
   * Constructs a new {@link AddGroupEvent} with the specified initiator, metalake name, and group
   * information.
   *
   * @param initiator the user who initiated the add-group request.
   * @param metalake the name of the metalake where the group was added.
   * @param addedGroupInfo the information about the group that was added.
   */
  public AddGroupEvent(String initiator, String metalake, GroupInfo addedGroupInfo) {
    super(initiator, NameIdentifierUtil.ofGroup(metalake, addedGroupInfo.name()));

    this.addedGroupInfo = addedGroupInfo;
  }

  /**
   * Retrieves the {@link GroupInfo} of the group that was added to the metalake.
   *
   * @return the {@link GroupInfo} object containing details of the added group.
   */
  public GroupInfo addedGroupInfo() {
    return addedGroupInfo;
  }

  /**
   * Returns the operation type for this event.
   *
   * @return the operation type for this event.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ADD_GROUP;
  }
}
