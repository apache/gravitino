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

/** Represents an event triggered before retrieving a user from a specific metalake. */
@DeveloperApi
public class GetUserPreEvent extends UserPreEvent {
  private final String userName;

  /**
   * Constructs a new {@link GetUserPreEvent} instance with the specified user, identifier, and user
   * info.
   *
   * @param initiator The name of the user who initiated the get-user request.
   * @param metalake The name of the metalake where the user is being retrieved from.
   * @param userName The username that is requested to be retrieved.
   */
  public GetUserPreEvent(String initiator, String metalake, String userName) {
    super(initiator, NameIdentifierUtil.ofUser(metalake, userName));
    this.userName = userName;
  }

  /**
   * Returns the username for the user being retrieved.
   *
   * @return The username that is requested to be retrieved.
   */
  public String userName() {
    return userName;
  }

  /**
   * Returns the operation type for this event.
   *
   * @return the operation type for this event.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_USER;
  }
}
