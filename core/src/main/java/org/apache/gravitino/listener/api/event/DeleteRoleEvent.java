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

/** Represents an event triggered after a role is successfully deleted. */
@DeveloperApi
public class DeleteRoleEvent extends RoleEvent {
  private final String roleName;
  private final boolean isExists;

  /**
   * Constructs a new {@code DeleteRoleEvent} instance.
   *
   * @param initiator the user who initiated the event.
   * @param metalake the metalake name where the role was deleted.
   * @param roleName the name of the deleted role.
   * @param isExists a flag indicating whether the role existed at the time of deletion.
   */
  public DeleteRoleEvent(String initiator, String metalake, String roleName, boolean isExists) {
    super(initiator, NameIdentifierUtil.ofRole(metalake, roleName));

    this.roleName = roleName;
    this.isExists = isExists;
  }

  /**
   * Returns a flag indicating whether the role existed at the time of deletion.
   *
   * @return {@code true} if the role existed; {@code false} otherwise.
   */
  public boolean isExists() {
    return isExists;
  }

  /**
   * Returns the name of the deleted role.
   *
   * @return the name of the deleted role.
   */
  public String roleName() {
    return roleName;
  }

  /**
   * Returns the operation type of this event.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.DELETE_ROLE;
  }
}
