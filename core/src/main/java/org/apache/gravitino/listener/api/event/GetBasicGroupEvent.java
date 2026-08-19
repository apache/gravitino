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
import org.apache.gravitino.listener.api.info.BasicGroupInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered after successfully retrieving a basic group from a metalake. */
@DeveloperApi
public class GetBasicGroupEvent extends GroupEvent {
  private final BasicGroupInfo loadedGroupInfo;

  /**
   * Constructs a new {@link GetBasicGroupEvent} instance.
   *
   * @param initiator the user who initiated the request to get the basic group.
   * @param metalake the name of the metalake from which the group is retrieved.
   * @param loadedGroupInfo the basic group information of the retrieved group.
   */
  public GetBasicGroupEvent(String initiator, String metalake, BasicGroupInfo loadedGroupInfo) {
    super(initiator, NameIdentifierUtil.ofGroup(metalake, loadedGroupInfo.name()));
    this.loadedGroupInfo = loadedGroupInfo;
  }

  /** Returns the basic group information of the group successfully retrieved from the metalake. */
  public BasicGroupInfo loadedGroupInfo() {
    return loadedGroupInfo;
  }

  @Override
  public OperationType operationType() {
    return OperationType.GET_BASIC_GROUP;
  }
}
