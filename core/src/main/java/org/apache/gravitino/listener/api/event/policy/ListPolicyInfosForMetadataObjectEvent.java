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
import org.apache.gravitino.listener.api.event.ListEvent;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.utils.MetadataObjectUtil;

/**
 * Represents an event that is triggered upon successfully listing policy infos associated with a
 * metadata object.
 */
@DeveloperApi
public final class ListPolicyInfosForMetadataObjectEvent extends PolicyEvent implements ListEvent {
  private final MetadataObject metadataObject;
  private final int count;

  /**
   * Constructs an instance of {@code ListPolicyInfosForMetadataObjectEvent}.
   *
   * @param user The username of the individual who initiated the list policy infos operation.
   * @param metalake The metalake from which the policy infos were listed.
   * @param metadataObject The metadata object for which policy infos were listed.
   * @param count The number of policy infos returned by the list operation.
   */
  public ListPolicyInfosForMetadataObjectEvent(
      String user, String metalake, MetadataObject metadataObject, int count) {
    super(user, MetadataObjectUtil.toEntityIdent(metalake, metadataObject));
    this.metadataObject = metadataObject;
    this.count = count;
  }

  /**
   * Constructs an instance of {@code ListPolicyInfosForMetadataObjectEvent} without a count.
   *
   * @param user The username of the individual who initiated the list policy infos operation.
   * @param metalake The metalake from which the policy infos were listed.
   * @param metadataObject The metadata object for which policy infos were listed.
   * @deprecated Use {@link #ListPolicyInfosForMetadataObjectEvent(String, String, MetadataObject,
   *     int)} instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ListPolicyInfosForMetadataObjectEvent(
      String user, String metalake, MetadataObject metadataObject) {
    this(user, metalake, metadataObject, -1);
  }

  /** {@inheritDoc} */
  @Override
  public int resultCount() {
    return count;
  }

  /**
   * Returns the metadata object for which policy infos were listed.
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
