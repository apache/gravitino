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

/** Represents an event that is triggered upon successfully retrieving a policy. */
@DeveloperApi
public final class GetPolicyEvent extends PolicyEvent {
  private final PolicyInfo policyInfo;

  /**
   * Constructs an instance of {@code GetPolicyEvent}.
   *
   * @param user The username of the individual who initiated the policy retrieval.
   * @param identifier The identifier of the retrieved policy.
   * @param policyInfo The {@link PolicyInfo} object representing the retrieved policy.
   */
  public GetPolicyEvent(String user, NameIdentifier identifier, PolicyInfo policyInfo) {
    super(user, identifier);
    this.policyInfo = policyInfo;
  }

  /**
   * Returns the {@link PolicyInfo} object representing the retrieved policy.
   *
   * @return the policy information.
   */
  public PolicyInfo policyInfo() {
    return policyInfo;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_POLICY;
  }
}
