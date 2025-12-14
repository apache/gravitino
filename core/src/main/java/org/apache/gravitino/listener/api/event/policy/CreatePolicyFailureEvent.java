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

/** Represents an event that is triggered when an attempt to create a policy fails. */
@DeveloperApi
public final class CreatePolicyFailureEvent extends PolicyFailureEvent {
  private final PolicyInfo createPolicyRequest;

  /**
   * Constructs an instance of {@code CreatePolicyFailureEvent}.
   *
   * @param user The username of the individual who initiated the policy creation.
   * @param identifier The identifier of the policy that failed to be created.
   * @param exception The exception that was encountered during the policy creation attempt.
   * @param createPolicyRequest The policy creation request information.
   */
  public CreatePolicyFailureEvent(
      String user, NameIdentifier identifier, Exception exception, PolicyInfo createPolicyRequest) {
    super(user, identifier, exception);
    this.createPolicyRequest = createPolicyRequest;
  }

  /**
   * Returns the policy creation request information.
   *
   * @return the policy creation request.
   */
  public PolicyInfo createPolicyRequest() {
    return createPolicyRequest;
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
