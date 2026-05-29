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
 * Represents an event triggered after successfully listing group names from a specific metalake.
 */
@DeveloperApi
public class ListGroupNamesEvent extends GroupEvent implements ListEvent {

  private final int groupNameCount;

  /**
   * Constructs a new {@link ListGroupNamesEvent} with the specified initiator, metalake name and
   * count.
   *
   * @param initiator the user who initiated the list-group-names request.
   * @param metalake the name of the metalake from which group names are listed.
   * @param groupNameCount the number of group names returned by the list operation.
   */
  public ListGroupNamesEvent(String initiator, String metalake, int groupNameCount) {
    super(initiator, NameIdentifierUtil.ofMetalake(metalake));
    this.groupNameCount = groupNameCount;
  }

  /**
   * Constructs a new {@link ListGroupNamesEvent} without a count.
   *
   * @param initiator the user who initiated the list-group-names request.
   * @param metalake the name of the metalake from which group names are listed.
   * @deprecated Use {@link #ListGroupNamesEvent(String, String, int)} instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ListGroupNamesEvent(String initiator, String metalake) {
    this(initiator, metalake, -1);
  }

  /** {@inheritDoc} */
  @Override
  public int resultCount() {
    return groupNameCount;
  }

  /**
   * Returns the operation type for this event.
   *
   * @return the operation type for this event.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_GROUP_NAMES;
  }
}
