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
package org.apache.gravitino.listener.api.event.domain;

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.ListEvent;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.utils.MetadataObjectUtil;

/** The success event for ListDomainsForMetadataObject. */
@DeveloperApi
public final class ListDomainsForMetadataObjectEvent extends DomainEvent implements ListEvent {
  private final MetadataObject metadataObject;
  private final int resultCount;

  /**
   * Creates the event.
   *
   * @param user The initiating user.
   * @param metalake The metalake name.
   * @param metadataObject The leaf metadata object.
   * @param resultCount The number of dispatcher results before REST authorization filtering, or -1
   *     if unavailable.
   */
  public ListDomainsForMetadataObjectEvent(
      String user, String metalake, MetadataObject metadataObject, int resultCount) {
    super(user, MetadataObjectUtil.toEntityIdent(metalake, metadataObject));
    this.metadataObject = metadataObject;
    this.resultCount = resultCount;
  }

  /**
   * @return The leaf metadata object.
   */
  public MetadataObject metadataObject() {
    return metadataObject;
  }

  /**
   * @return The number of dispatcher results before REST authorization filtering, or -1 if
   *     unavailable.
   */
  @Override
  public int resultCount() {
    return resultCount;
  }

  /** {@inheritDoc} */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_DOMAINS_FOR_METADATA_OBJECT;
  }
}
