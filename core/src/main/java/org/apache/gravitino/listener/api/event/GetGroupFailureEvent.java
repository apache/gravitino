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

/**
 * Represents an event triggered when an attempt to retrieve a group from the metalake fails due to
 * an exception.
 */
@DeveloperApi
public class GetGroupFailureEvent extends GroupFailureEvent {
  private final String groupName;

  /**
   * Creates a new instance of {@code GetGroupFailureEvent}.
   *
   * @param initiator the user who initiated the operation
   * @param metalake the name of the metalake from which the group retrieval was attempted
   * @param exception the exception encountered during the operation
   * @param groupName the name of the group that failed to be retrieved
   */
  public GetGroupFailureEvent(
      String initiator, String metalake, Exception exception, String groupName) {
    super(initiator, NameIdentifierUtil.ofGroup(metalake, groupName), exception);

    this.groupName = groupName;
  }

  /**
   * Retrieves the name of the group that failed to be retrieved.
   *
   * @return the group name involved in the failure event
   */
  public String groupName() {
    return groupName;
  }

  /**
   * Returns the operation type for this event.
   *
   * @return the operation type for this event.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_GROUP;
  }
}
