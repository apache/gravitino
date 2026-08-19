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

/** Represents an event triggered when retrieving a basic user from the metalake fails. */
@DeveloperApi
public class GetBasicUserFailureEvent extends UserFailureEvent {
  private final String userName;

  /**
   * Constructs a new {@code GetBasicUserFailureEvent} instance.
   *
   * @param user the user who initiated the operation
   * @param metalake the name of the metalake from which the user was attempted to be retrieved
   * @param exception the exception encountered during the operation
   * @param userName the name of the user that failed to be retrieved
   */
  public GetBasicUserFailureEvent(
      String user, String metalake, Exception exception, String userName) {
    super(user, NameIdentifierUtil.ofUser(metalake, userName), exception);
    this.userName = userName;
  }

  /** Returns the name of the user that failed to be retrieved. */
  public String userName() {
    return userName;
  }

  @Override
  public OperationType operationType() {
    return OperationType.GET_BASIC_USER;
  }
}
