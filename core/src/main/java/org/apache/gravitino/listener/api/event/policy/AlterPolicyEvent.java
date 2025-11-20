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
import org.apache.gravitino.listener.api.info.PolicyInfo;
import org.apache.gravitino.policy.PolicyChange;

/** Represents an event triggered upon the successful alteration of a policy. */
@DeveloperApi
public final class AlterPolicyEvent extends PolicyEvent {
  private final PolicyInfo updatedPolicyInfo;
  private final PolicyChange[] policyChanges;

  /**
   * Constructs an instance of {@code AlterPolicyEvent}, encapsulating the key details about the
   * successful alteration of a policy.
   *
   * @param user The username of the individual responsible for initiating the policy alteration.
   * @param identifier The identifier of the altered policy.
   * @param policyChanges An array of {@link PolicyChange} objects representing the specific changes
   *     applied to the policy during the alteration process.
   * @param updatedPolicyInfo The post-alteration state of the policy.
   */
  public AlterPolicyEvent(
      String user,
      NameIdentifier identifier,
      PolicyChange[] policyChanges,
      PolicyInfo updatedPolicyInfo) {
    super(user, identifier);
    this.policyChanges = policyChanges != null ? policyChanges.clone() : null;
    this.updatedPolicyInfo = updatedPolicyInfo;
  }

  /**
   * Retrieves the specific changes that were made to the policy during the alteration process.
   *
   * @return An array of {@link PolicyChange} objects detailing each modification applied to the
   *     policy.
   */
  public PolicyChange[] policyChanges() {
    return policyChanges;
  }

  /**
   * Retrieves the final state of the policy as it was returned to the user after successful
   * alteration.
   *
   * @return A {@link PolicyInfo} instance encapsulating the comprehensive details of the newly
   *     altered policy.
   */
  public PolicyInfo updatedPolicyInfo() {
    return updatedPolicyInfo;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ALTER_POLICY;
  }
}
