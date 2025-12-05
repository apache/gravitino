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
 * Represents an event triggered when an attempt to list usernames from a metalake fails due to an
 * exception.
 */
@DeveloperApi
public class ListUserNamesFailureEvent extends UserFailureEvent {
  /**
   * Constructs a new {@code ListUserNamesFailureEvent} instance.
   *
   * @param user the user who initiated the operation
   * @param metalake the name of the metalake where the operation was attempted
   * @param exception the exception encountered during the operation
   */
  public ListUserNamesFailureEvent(String user, String metalake, Exception exception) {
    super(user, NameIdentifierUtil.ofMetalake(metalake), exception);
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
