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

/** Represents an event triggered before add a user to a metalake. */
@DeveloperApi
public class AddUserPreEvent extends UserPreEvent {
  private final String userName;

  /**
   * Construct a new {@link AddUserPreEvent} instance with initiator, identifier and username.
   *
   * @param initiator the user who initiated the add-user request.
   * @param identifier the identifier of the metalake which the user is being added to.
   * @param userName the username which is requested to be added to the metalake.
   */
  public AddUserPreEvent(String initiator, NameIdentifier identifier, String userName) {
    super(initiator, identifier);

    this.userName = userName;
  }

  /**
   * Returns the user information which is being added to the metalake.
   *
   * @return the username which is requested to be added to the metalake.
   */
  public String userName() {
    return userName;
  }

  /**
   * Returns the operation type of this event.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ADD_USER;
  }
}
