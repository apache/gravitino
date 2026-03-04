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

/** Represents an event that is activated upon the successful creation of a policy. */
@DeveloperApi
public final class CreatePolicyEvent extends PolicyEvent {
  private final PolicyInfo createdPolicyInfo;

  /**
   * Constructs an instance of {@code CreatePolicyEvent}, capturing essential details about the
   * successful creation of a policy.
   *
   * @param user The username of the individual who initiated the policy creation.
   * @param identifier The identifier of the created policy.
   * @param createdPolicyInfo The final state of the policy post-creation.
   */
  public CreatePolicyEvent(String user, NameIdentifier identifier, PolicyInfo createdPolicyInfo) {
    super(user, identifier);
    this.createdPolicyInfo = createdPolicyInfo;
  }

  /**
   * Provides the final state of the policy as it is presented to the user following the successful
   * creation.
   *
   * @return A {@link PolicyInfo} object that encapsulates the detailed characteristics of the newly
   *     created policy.
   */
  public PolicyInfo createdPolicyInfo() {
    return createdPolicyInfo;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.CREATE_POLICY;
  }
}
