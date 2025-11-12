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

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.utils.MetadataObjectUtil;

/**
 * Represents an event that is triggered when an attempt to retrieve a policy for a metadata object
 * fails.
 */
@DeveloperApi
public final class GetPolicyForMetadataObjectFailureEvent extends PolicyFailureEvent {
  private final MetadataObject metadataObject;
  private final String policyName;

  /**
   * Constructs an instance of {@code GetPolicyForMetadataObjectFailureEvent}.
   *
   * @param user The username of the individual who initiated the policy retrieval.
   * @param metalake The metalake from which the policy was to be retrieved.
   * @param metadataObject The metadata object for which the policy was to be retrieved.
   * @param policyName The name of the policy that was to be retrieved.
   * @param exception The exception that was encountered during the policy retrieval attempt.
   */
  public GetPolicyForMetadataObjectFailureEvent(
      String user,
      String metalake,
      MetadataObject metadataObject,
      String policyName,
      Exception exception) {
    super(user, MetadataObjectUtil.toEntityIdent(metalake, metadataObject), exception);
    this.metadataObject = metadataObject;
    this.policyName = policyName;
  }

  /**
   * Returns the metadata object for which the policy retrieval was attempted.
   *
   * @return the metadata object.
   */
  public MetadataObject metadataObject() {
    return metadataObject;
  }

  /**
   * Returns the name of the policy that was to be retrieved.
   *
   * @return the policy name.
   */
  public String policyName() {
    return policyName;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_POLICY_FOR_METADATA_OBJECT;
  }
}
