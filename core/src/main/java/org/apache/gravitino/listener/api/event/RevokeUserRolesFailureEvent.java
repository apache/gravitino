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
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Represents an event triggered when an attempt to revoke roles from a user fails due to an
 * exception.
 */
@DeveloperApi
public class RevokeUserRolesFailureEvent extends UserFailureEvent {
  private final String userName;
  private final List<String> roles;

  /**
   * Constructs a new {@code RevokeUserRolesFailureEvent} instance.
   *
   * @param initiator the user who initiated the operation
   * @param metalake the name of the metalake where the operation was attempted
   * @param exception the exception encountered during the operation
   * @param userName the name of the user from whom the roles were attempted to be revoked
   * @param roles the list of roles that were attempted to be revoked from the user
   */
  public RevokeUserRolesFailureEvent(
      String initiator, String metalake, Exception exception, String userName, List<String> roles) {
    super(initiator, NameIdentifierUtil.ofUser(metalake, userName), exception);
    this.userName = userName;
    this.roles = roles;
  }

  /**
   * Returns the name of the user from whom the roles were attempted to be revoked.
   *
   * @return the username involved in the failure event
   */
  public String userName() {
    return userName;
  }

  /**
   * Returns the list of roles that were attempted to be revoked from the user.
   *
   * @return the list of roles involved in the failure event
   */
  public List<String> roles() {
    return roles;
  }

  /**
   * Returns the operation type of this event.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.REVOKE_USER_ROLES;
  }
}
