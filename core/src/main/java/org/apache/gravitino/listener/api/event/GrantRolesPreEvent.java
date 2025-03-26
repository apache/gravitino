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

import java.util.List;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated before a role is successfully granted to a user or group.
 */
@DeveloperApi
public class GrantRolesPreEvent extends RolePreEvent {
  private final List<String> roles;
  private Optional<String> userName;
  private Optional<String> groupName;

  /**
   * Constructs a new {@link GrantRolesPreEvent} instance with the specified initiator, identifier,
   * and user and group names. Only one of the user or group name should be specified.
   *
   * @param initiator the user who initiated the event.
   * @param identifier the identifier of the metalake which is being operated on.
   * @param roles the list of roles to be granted.
   * @param userName the name of the user to whom the role is being granted.
   * @param groupName the name of the group to whom the role is being granted.
   */
  public GrantRolesPreEvent(
      String initiator,
      NameIdentifier identifier,
      List<String> roles,
      String userName,
      String groupName) {
    super(initiator, identifier);

    this.roles = roles;
    this.userName = Optional.ofNullable(userName);
    this.groupName = Optional.ofNullable(groupName);
  }

  /**
   * Returns the list of roles to be granted.
   *
   * @return the list of roles to be granted.
   */
  public List<String> roles() {
    return roles;
  }

  /**
   * Returns the name of the user to whom the role is being granted.
   *
   * @return If the username is present, returns an Optional containing the username. Otherwise,
   *     returns {@code Optional.empty()}
   */
  public Optional<String> userName() {
    return userName;
  }

  /**
   * Returns the name of the group to whom the role is being granted.
   *
   * @return If the group name is present, returns an Optional containing the group name. Otherwise,
   *     returns {@code Optional.empty()}
   */
  public Optional<String> groupName() {
    return groupName;
  }

  /**
   * Returns the operation type of this event.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GRANT_ROLES;
  }
}
