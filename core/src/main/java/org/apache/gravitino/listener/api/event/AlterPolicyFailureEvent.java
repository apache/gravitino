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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.policy.PolicyChange;

/** Represents an event triggered when altering a policy fails. */
@DeveloperApi
public final class AlterPolicyFailureEvent extends PolicyFailureEvent {
  private final PolicyChange[] policyChanges;

  /**
   * Constructs an AlterPolicyFailureEvent.
   *
   * @param user The user who attempted to alter the policy.
   * @param identifier The identifier of the policy that failed to be altered.
   * @param exception The exception that caused the failure.
   * @param policyChanges The changes that were attempted.
   */
  public AlterPolicyFailureEvent(
      String user, NameIdentifier identifier, Exception exception, PolicyChange[] policyChanges) {
    super(user, identifier, exception);
    this.policyChanges = policyChanges;
  }

  /**
   * Returns the changes that were attempted.
   *
   * @return The policy changes.
   */
  public PolicyChange[] policyChanges() {
    return policyChanges;
  }

  /**
   * Returns the operation type.
   *
   * @return The operation type (ALTER_POLICY).
   */
  @Override
  public OperationType operationType() {
    return OperationType.ALTER_POLICY;
  }
}
