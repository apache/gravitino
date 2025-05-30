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
 * Represents an event triggered when an attempt to get a tag from the database fails due to an
 * exception.
 */
@DeveloperApi
public class GetTagFailureEvent extends TagFailureEvent {
  /**
   * Constructs a new {@code GetTagFailureEvent} instance.
   *
   * @param user The user who initiated the get tag operation.
   * @param metalake The metalake name where the tag resides.
   * @param name The name of the tag to get.
   * @param exception The exception encountered during the get tag operation, providing insights
   *     into the reasons behind the operation's failure.
   */
  public GetTagFailureEvent(String user, String metalake, String name, Exception exception) {
    super(user, NameIdentifierUtil.ofTag(metalake, name), exception);
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_TAG;
  }
}
