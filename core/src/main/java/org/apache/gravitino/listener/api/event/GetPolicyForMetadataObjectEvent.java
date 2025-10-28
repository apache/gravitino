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

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.info.PolicyInfo;
import org.apache.gravitino.utils.MetadataObjectUtil;

/** Represents an event triggered after successfully retrieving a policy for a metadata object. */
@DeveloperApi
public final class GetPolicyForMetadataObjectEvent extends PolicyEvent {
  private final MetadataObject metadataObject;
  private final PolicyInfo policyInfo;

  /**
   * Constructs a GetPolicyForMetadataObjectEvent.
   *
   * @param user The user who retrieved the policy for the metadata object.
   * @param metalake The metalake from which the policy was retrieved.
   * @param metadataObject The metadata object for which the policy was retrieved.
   * @param policyInfo The information about the retrieved policy.
   */
  public GetPolicyForMetadataObjectEvent(
      String user, String metalake, MetadataObject metadataObject, PolicyInfo policyInfo) {
    super(user, MetadataObjectUtil.toEntityIdent(metalake, metadataObject));
    this.metadataObject = metadataObject;
    this.policyInfo = policyInfo;
  }

  /**
   * Returns the metadata object for which the policy was retrieved.
   *
   * @return The metadata object.
   */
  public MetadataObject metadataObject() {
    return metadataObject;
  }

  /**
   * Returns the information about the retrieved policy.
   *
   * @return The policy information.
   */
  public PolicyInfo policyInfo() {
    return policyInfo;
  }

  /**
   * Returns the operation type.
   *
   * @return The operation type (GET_POLICY_FOR_METADATA_OBJECT).
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_POLICY_FOR_METADATA_OBJECT;
  }
}
