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

/** Represents an event triggered after successfully counting groups in a metalake. */
@DeveloperApi
public class CountGroupsEvent extends GroupEvent {

  private final long count;

  /**
   * Creates a new {@link CountGroupsEvent}.
   *
   * @param initiator the user who initiated the request.
   * @param metalake the metalake name.
   * @param count the total number of groups.
   */
  public CountGroupsEvent(String initiator, String metalake, long count) {
    super(initiator, NameIdentifierUtil.ofMetalake(metalake));
    this.count = count;
  }

  /**
   * Returns the total number of groups.
   *
   * @return the group count.
   */
  public long count() {
    return count;
  }

  @Override
  public OperationType operationType() {
    return OperationType.COUNT_GROUPS;
  }
}
