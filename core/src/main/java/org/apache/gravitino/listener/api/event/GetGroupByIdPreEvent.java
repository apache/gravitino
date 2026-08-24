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
import org.apache.gravitino.authorization.AuthorizationUtils;

/** Represents an event triggered before retrieving a group by Gravitino-assigned id. */
@DeveloperApi
public class GetGroupByIdPreEvent extends GroupPreEvent {
  private final long groupId;

  /**
   * Creates a new {@link GetGroupByIdPreEvent}.
   *
   * @param initiator The user who initiated the request.
   * @param metalake The metalake name.
   * @param groupId The Gravitino-assigned id of the group.
   */
  public GetGroupByIdPreEvent(String initiator, String metalake, long groupId) {
    super(initiator, AuthorizationUtils.ofGroupId(metalake, groupId));
    this.groupId = groupId;
  }

  /**
   * Returns the Gravitino-assigned id of the group being retrieved.
   *
   * @return The group id.
   */
  public long groupId() {
    return groupId;
  }

  @Override
  public OperationType operationType() {
    return OperationType.GET_GROUP_BY_ID;
  }
}
