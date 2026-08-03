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
import org.apache.gravitino.listener.api.info.GroupInfo;

/** Represents an event triggered after successfully retrieving a group by Gravitino-assigned id. */
@DeveloperApi
public class GetGroupByIdEvent extends GroupEvent {
  private final GroupInfo loadedGroupInfo;

  /**
   * Creates a new {@link GetGroupByIdEvent}.
   *
   * @param initiator The user who initiated the request.
   * @param metalake The metalake name.
   * @param loadedGroupInfo The retrieved group information.
   */
  public GetGroupByIdEvent(String initiator, String metalake, GroupInfo loadedGroupInfo) {
    super(
        initiator,
        AuthorizationUtils.ofGroupId(
            metalake,
            loadedGroupInfo
                .id()
                .orElseThrow(
                    () ->
                        new IllegalStateException("Group id is required for GetGroupByIdEvent"))));
    this.loadedGroupInfo = loadedGroupInfo;
  }

  /**
   * Returns the retrieved group information.
   *
   * @return The group information.
   */
  public GroupInfo loadedGroupInfo() {
    return loadedGroupInfo;
  }

  @Override
  public OperationType operationType() {
    return OperationType.GET_GROUP_BY_ID;
  }
}
