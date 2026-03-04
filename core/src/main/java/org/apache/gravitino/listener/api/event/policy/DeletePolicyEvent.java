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

package org.apache.gravitino.listener.api.event.policy;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;

/** Represents an event that is triggered upon successfully deleting a policy. */
@DeveloperApi
public final class DeletePolicyEvent extends PolicyEvent {
  private final boolean isExists;

  /**
   * Constructs an instance of {@code DeletePolicyEvent}.
   *
   * @param user The username of the individual who initiated the policy deletion.
   * @param identifier The identifier of the deleted policy.
   * @param isExists Whether the policy existed before deletion.
   */
  public DeletePolicyEvent(String user, NameIdentifier identifier, boolean isExists) {
    super(user, identifier);
    this.isExists = isExists;
  }

  /**
   * Returns whether the policy existed before deletion.
   *
   * @return true if the policy existed, false otherwise.
   */
  public boolean isExists() {
    return isExists;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.DELETE_POLICY;
  }
}
