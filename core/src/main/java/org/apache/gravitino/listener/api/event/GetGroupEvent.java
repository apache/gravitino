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

/**
 * Represents an event triggered after a group is successfully retrieved from a specific metalake.
 */
@DeveloperApi
public class GetGroupEvent extends GroupEvent {
  private final GroupInfo loadedGroupInfo;

  /**
   * Constructs a new {@link GetGroupEvent} with the specified initiator, metalake name, and group
   * information.
   *
   * @param initiator the user who initiated the group retrieval request.
   * @param metalake the name of the metalake from which the group is retrieved.
   * @param loadedGroupInfo the information of the group that was retrieved.
   */
  public GetGroupEvent(String initiator, String metalake, GroupInfo loadedGroupInfo) {
    super(initiator, NameIdentifierUtil.ofGroup(metalake, loadedGroupInfo.name()));

    this.loadedGroupInfo = loadedGroupInfo;
  }

  /**
   * Retrieves the {@link GroupInfo} of the group that was successfully retrieved from the metalake.
   *
   * @return the {@link GroupInfo} object containing details of the retrieved group.
   */
  public GroupInfo loadedGroupInfo() {
    return loadedGroupInfo;
  }

  /**
   * Returns the operation type for this event.
   *
   * @return the operation type for this event.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_GROUP;
  }
}
