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

/** Represents an event triggered after successfully listing usernames from a specific metalake. */
@DeveloperApi
public class ListUserNamesEvent extends UserEvent implements ListEvent {

  private final int userNameCount;

  /**
   * Constructs a new {@link ListUserNamesEvent} instance with the specified initiator, metalake
   * name and count.
   *
   * @param initiator the user who initiated the request to list usernames.
   * @param metalake the name of the metalake from which the usernames are listed.
   * @param userNameCount the number of usernames returned by the list operation.
   */
  public ListUserNamesEvent(String initiator, String metalake, int userNameCount) {
    super(initiator, NameIdentifier.of(metalake));
    this.userNameCount = userNameCount;
  }

  /**
   * Constructs a new {@link ListUserNamesEvent} instance without a count.
   *
   * @param initiator the user who initiated the request to list usernames.
   * @param metalake the name of the metalake from which the usernames are listed.
   * @deprecated Use {@link #ListUserNamesEvent(String, String, int)} instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ListUserNamesEvent(String initiator, String metalake) {
    this(initiator, metalake, -1);
  }

  /** {@inheritDoc} */
  @Override
  public int resultCount() {
    return userNameCount;
  }

  /**
   * Returns the operation type for this event.
   *
   * @return the operation type for this event.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_USER_NAMES;
  }
}
