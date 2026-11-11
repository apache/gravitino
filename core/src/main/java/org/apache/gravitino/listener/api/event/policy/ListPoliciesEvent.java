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
import org.apache.gravitino.listener.api.event.ListEvent;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Represents an event that is triggered upon successfully listing policies from a specific
 * metalake.
 */
@DeveloperApi
public class ListPoliciesEvent extends PolicyEvent implements ListEvent {

  private final int count;

  /**
   * Constructs an instance of {@code ListPoliciesEvent}.
   *
   * @param initiator The username of the individual who initiated the list-policies request.
   * @param metalake The name of the metalake from which the policies were listed.
   * @param count The number of policies returned by the list operation.
   */
  public ListPoliciesEvent(String initiator, String metalake, int count) {
    super(initiator, NameIdentifierUtil.ofMetalake(metalake));
    this.count = count;
  }

  /**
   * Constructs an instance of {@code ListPoliciesEvent} without a count.
   *
   * @param initiator The username of the individual who initiated the list-policies request.
   * @param metalake The name of the metalake from which the policies were listed.
   * @deprecated Use {@link #ListPoliciesEvent(String, String, int)} instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ListPoliciesEvent(String initiator, String metalake) {
    this(initiator, metalake, -1);
  }

  /** {@inheritDoc} */
  @Override
  public int resultCount() {
    return count;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_POLICY;
  }
}
