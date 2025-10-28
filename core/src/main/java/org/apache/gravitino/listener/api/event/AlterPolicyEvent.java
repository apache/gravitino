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
import org.apache.gravitino.listener.api.info.PolicyInfo;
import org.apache.gravitino.policy.PolicyChange;

/** Represents an event triggered after successfully altering a policy. */
@DeveloperApi
public final class AlterPolicyEvent extends PolicyEvent {
  private final PolicyInfo updatedPolicyInfo;
  private final PolicyChange[] policyChanges;

  /**
   * Constructs an AlterPolicyEvent.
   *
   * @param user The user who altered the policy.
   * @param identifier The identifier of the altered policy.
   * @param policyChanges The changes applied to the policy.
   * @param updatedPolicyInfo The information about the updated policy.
   */
  public AlterPolicyEvent(
      String user,
      NameIdentifier identifier,
      PolicyChange[] policyChanges,
      PolicyInfo updatedPolicyInfo) {
    super(user, identifier);
    this.policyChanges = policyChanges;
    this.updatedPolicyInfo = updatedPolicyInfo;
  }

  /**
   * Returns the changes applied to the policy.
   *
   * @return The policy changes.
   */
  public PolicyChange[] policyChanges() {
    return policyChanges;
  }

  /**
   * Returns the information about the updated policy.
   *
   * @return The updated policy information.
   */
  public PolicyInfo updatedPolicyInfo() {
    return updatedPolicyInfo;
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
