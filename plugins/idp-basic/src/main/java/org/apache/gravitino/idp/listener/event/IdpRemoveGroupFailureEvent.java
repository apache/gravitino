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

package org.apache.gravitino.idp.listener.event;

import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.idp.listener.IdpNameIdentifierUtil;
import org.apache.gravitino.listener.api.event.OperationType;

/** Failure event for removing a built-in IdP group. */
@DeveloperApi
public class IdpRemoveGroupFailureEvent extends IdpGroupFailureEvent {
  private final String groupName;
  private final boolean force;

  public IdpRemoveGroupFailureEvent(
      String initiator, String groupName, boolean force, Exception exception) {
    super(initiator, IdpNameIdentifierUtil.ofGroup(groupName), exception);
    this.groupName = groupName;
    this.force = force;
  }

  public String groupName() {
    return groupName;
  }

  public boolean force() {
    return force;
  }

  @Override
  public OperationType operationType() {
    return OperationType.REMOVE_GROUP;
  }
}
