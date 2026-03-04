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

import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Represents an event that is triggered when an attempt to list policy infos from a metalake fails.
 */
@DeveloperApi
public class ListPolicyInfosFailureEvent extends PolicyFailureEvent {

  /**
   * Constructs an instance of {@code ListPolicyInfosFailureEvent}.
   *
   * @param initiator The username of the individual who initiated the list-policy-infos request.
   * @param metalake The name of the metalake from which policy infos were to be listed.
   * @param exception The exception that was encountered during the list-policy-infos attempt.
   */
  public ListPolicyInfosFailureEvent(String initiator, String metalake, Exception exception) {
    super(initiator, NameIdentifierUtil.ofMetalake(metalake), exception);
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_POLICY_INFO;
  }
}
