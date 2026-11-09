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

import java.util.Collections;
import java.util.List;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.idp.listener.IdpNameIdentifierUtil;
import org.apache.gravitino.listener.api.event.OperationType;

/** Pre-event for changing built-in IdP group membership. */
@DeveloperApi
public class IdpChangeGroupMembershipPreEvent extends IdpGroupPreEvent {
  private final String groupName;
  private final List<String> usersToAdd;
  private final List<String> usersToRemove;

  public IdpChangeGroupMembershipPreEvent(
      String initiator, String groupName, List<String> usersToAdd, List<String> usersToRemove) {
    super(initiator, IdpNameIdentifierUtil.ofGroup(groupName));
    this.groupName = groupName;
    this.usersToAdd = usersToAdd == null ? Collections.emptyList() : List.copyOf(usersToAdd);
    this.usersToRemove =
        usersToRemove == null ? Collections.emptyList() : List.copyOf(usersToRemove);
  }

  public String groupName() {
    return groupName;
  }

  public List<String> usersToAdd() {
    return usersToAdd;
  }

  public List<String> usersToRemove() {
    return usersToRemove;
  }

  @Override
  public OperationType operationType() {
    return OperationType.ALTER_GROUP;
  }
}
