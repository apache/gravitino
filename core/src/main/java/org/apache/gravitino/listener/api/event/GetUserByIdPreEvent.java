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

/** Represents an event triggered before retrieving a user by Gravitino-assigned id. */
@DeveloperApi
public class GetUserByIdPreEvent extends UserPreEvent {
  private final long userId;

  /**
   * Creates a new {@link GetUserByIdPreEvent}.
   *
   * @param initiator The user who initiated the request.
   * @param metalake The metalake name.
   * @param userId The Gravitino-assigned id of the user.
   */
  public GetUserByIdPreEvent(String initiator, String metalake, long userId) {
    super(initiator, AuthorizationUtils.ofUserId(metalake, userId));
    this.userId = userId;
  }

  /**
   * Returns the Gravitino-assigned id of the user being retrieved.
   *
   * @return The user id.
   */
  public long userId() {
    return userId;
  }

  @Override
  public OperationType operationType() {
    return OperationType.GET_USER_BY_ID;
  }
}
