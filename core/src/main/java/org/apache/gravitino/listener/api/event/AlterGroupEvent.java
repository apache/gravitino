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
import org.apache.gravitino.authorization.GroupChange;
import org.apache.gravitino.listener.api.info.GroupInfo;

/** Represents an event triggered upon the successful alteration of a group. */
@DeveloperApi
public final class AlterGroupEvent extends GroupEvent {
  private final GroupInfo updatedGroupInfo;
  private final GroupChange[] groupChanges;

  /**
   * Creates a new {@link AlterGroupEvent}.
   *
   * @param initiator The user who initiated the request.
   * @param metalake The metalake name.
   * @param groupChanges The changes applied to the group.
   * @param updatedGroupInfo The post-alteration state of the group.
   */
  public AlterGroupEvent(
      String initiator, String metalake, GroupChange[] groupChanges, GroupInfo updatedGroupInfo) {
    super(initiator, AuthorizationUtils.ofGroupId(metalake, updatedGroupInfo.id()));
    this.groupChanges = groupChanges != null ? groupChanges.clone() : null;
    this.updatedGroupInfo = updatedGroupInfo;
  }

  /**
   * Returns the final state of the group after successful alteration.
   *
   * @return The updated group information.
   */
  public GroupInfo updatedGroupInfo() {
    return updatedGroupInfo;
  }

  /**
   * Returns the specific changes that were made to the group.
   *
   * @return An array of {@link GroupChange}, or null.
   */
  @Nullable
  public GroupChange[] changes() {
    return groupChanges;
  }

  @Override
  public OperationType operationType() {
    return OperationType.ALTER_GROUP;
  }
}
