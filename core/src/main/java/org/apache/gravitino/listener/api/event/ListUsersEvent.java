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

/** Represents an event triggered after successfully listing users from a specific metalake. */
@DeveloperApi
public class ListUsersEvent extends UserEvent implements ListEvent {

  private final int userCount;

  /**
   * Constructs a new {@link ListUsersEvent} instance with the specified initiator, metalake name
   * and user count.
   *
   * @param initiator the user who initiated the request to list users.
   * @param metalake the name of the metalake from which the users are listed.
   * @param userCount the number of users returned by the list operation.
   */
  public ListUsersEvent(String initiator, String metalake, int userCount) {
    super(initiator, NameIdentifier.of(metalake));
    this.userCount = userCount;
  }

  /**
   * Constructs a new {@link ListUsersEvent} instance without a count.
   *
   * @param initiator the user who initiated the request to list users.
   * @param metalake the name of the metalake from which the users are listed.
   * @deprecated Use {@link #ListUsersEvent(String, String, int)} instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ListUsersEvent(String initiator, String metalake) {
    this(initiator, metalake, -1);
  }

  /** {@inheritDoc} */
  @Override
  public int resultCount() {
    return userCount;
  }

  /**
   * Returns the operation type for this event.
   *
   * @return the operation type for this event.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_USERS;
  }
}
