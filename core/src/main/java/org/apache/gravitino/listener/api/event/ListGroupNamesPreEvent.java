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

/** Represents an event triggered before listing group names from a specific metalake. */
@DeveloperApi
public class ListGroupNamesPreEvent extends GroupPreEvent {

  /**
   * Constructs a new {@link ListGroupNamesPreEvent} with the specified initiator and metalake name.
   *
   * @param initiator the user who initiated the list-group-names request.
   * @param metalake the name of the metalake from which group names will be listed.
   */
  public ListGroupNamesPreEvent(String initiator, String metalake) {
    super(initiator, NameIdentifierUtil.ofMetalake(metalake));
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
