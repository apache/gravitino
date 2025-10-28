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

/** Represents an event triggered before creating a policy. */
@DeveloperApi
public final class CreatePolicyPreEvent extends PolicyPreEvent {
  private final PolicyInfo createPolicyRequest;

  /**
   * Constructs a CreatePolicyPreEvent.
   *
   * @param user The user who is creating the policy.
   * @param identifier The identifier of the policy to be created.
   * @param createPolicyRequest The policy creation request information.
   */
  public CreatePolicyPreEvent(
      String user, NameIdentifier identifier, PolicyInfo createPolicyRequest) {
    super(user, identifier);
    this.createPolicyRequest = createPolicyRequest;
  }

  /**
   * Returns the policy creation request information.
   *
   * @return The policy creation request.
   */
  public PolicyInfo createPolicyRequest() {
    return createPolicyRequest;
  }

  /**
   * Returns the operation type.
   *
   * @return The operation type (CREATE_POLICY).
   */
  @Override
  public OperationType operationType() {
    return OperationType.CREATE_POLICY;
  }
}
