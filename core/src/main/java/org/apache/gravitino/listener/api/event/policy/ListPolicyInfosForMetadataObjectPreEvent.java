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
 * Represents an event that is triggered before listing policy infos associated with a metadata
 * object.
 */
@DeveloperApi
public final class ListPolicyInfosForMetadataObjectPreEvent extends PolicyPreEvent {
  private final MetadataObject metadataObject;

  /**
   * Constructs an instance of {@code ListPolicyInfosForMetadataObjectPreEvent}.
   *
   * @param user The username of the individual who initiated the list policy infos operation.
   * @param metalake The metalake from which the policy infos will be listed.
   * @param metadataObject The metadata object for which policy infos will be listed.
   */
  public ListPolicyInfosForMetadataObjectPreEvent(
      String user, String metalake, MetadataObject metadataObject) {
    super(user, MetadataObjectUtil.toEntityIdent(metalake, metadataObject));
    this.metadataObject = metadataObject;
  }

  /**
   * Returns the metadata object for which policy infos will be listed.
   *
   * @return the metadata object.
   */
  public MetadataObject metadataObject() {
    return metadataObject;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_POLICY_INFOS_FOR_METADATA_OBJECT;
  }
}
