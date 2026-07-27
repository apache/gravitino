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

/** Represents an event triggered after removing a user by external id. */
@DeveloperApi
public class RemoveUserByExternalIdEvent extends UserEvent {
  private final String externalId;
  private final boolean isExists;

  /**
   * Creates a new {@link RemoveUserByExternalIdEvent}.
   *
   * @param initiator The user who initiated the request.
   * @param metalake The metalake name.
   * @param externalId The external identifier of the user.
   * @param isExists Whether the user existed and was removed.
   */
  public RemoveUserByExternalIdEvent(
      String initiator, String metalake, String externalId, boolean isExists) {
    super(initiator, AuthorizationUtils.ofUserExternalId(metalake, externalId));
    this.externalId = externalId;
    this.isExists = isExists;
  }

  /**
   * Returns the external identifier of the removed user.
   *
   * @return The external identifier.
   */
  public String externalId() {
    return externalId;
  }

  /**
   * Returns whether the user existed and was removed.
   *
   * @return {@code true} if the user was removed.
   */
  public boolean isExists() {
    return isExists;
  }

  @Override
  public OperationType operationType() {
    return OperationType.REMOVE_USER_BY_EXTERNAL_ID;
  }
}
