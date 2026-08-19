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
import org.apache.gravitino.listener.api.info.BasicUserInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered after successfully retrieving a basic user from a metalake. */
@DeveloperApi
public class GetBasicUserEvent extends UserEvent {
  private final BasicUserInfo loadedUserInfo;

  /**
   * Constructs a new {@link GetBasicUserEvent} instance.
   *
   * @param initiator the user who initiated the request to get the basic user.
   * @param metalake the name of the metalake from which the user is retrieved.
   * @param loadedUserInfo the basic user information of the retrieved user.
   */
  public GetBasicUserEvent(String initiator, String metalake, BasicUserInfo loadedUserInfo) {
    super(initiator, NameIdentifierUtil.ofUser(metalake, loadedUserInfo.name()));
    this.loadedUserInfo = loadedUserInfo;
  }

  /** Returns the basic user information of the user successfully retrieved from the metalake. */
  public BasicUserInfo loadedUserInfo() {
    return loadedUserInfo;
  }

  @Override
  public OperationType operationType() {
    return OperationType.GET_BASIC_USER;
  }
}
