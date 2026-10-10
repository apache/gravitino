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

/** Represents an event triggered before listing users with pagination. */
@DeveloperApi
public class ListUsersPagedPreEvent extends UserPreEvent {

  private final int offset;
  private final int limit;

  /**
   * Creates a new {@link ListUsersPagedPreEvent}.
   *
   * @param initiator the user who initiated the request.
   * @param metalake the metalake name.
   * @param offset the number of users to skip.
   * @param limit the maximum number of users to return.
   */
  public ListUsersPagedPreEvent(String initiator, String metalake, int offset, int limit) {
    super(initiator, NameIdentifier.of(metalake));
    this.offset = offset;
    this.limit = limit;
  }

  /**
   * Returns the pagination offset.
   *
   * @return the offset.
   */
  public int offset() {
    return offset;
  }

  /**
   * Returns the pagination limit.
   *
   * @return the limit.
   */
  public int limit() {
    return limit;
  }

  @Override
  public OperationType operationType() {
    return OperationType.LIST_USERS_PAGED;
  }
}
