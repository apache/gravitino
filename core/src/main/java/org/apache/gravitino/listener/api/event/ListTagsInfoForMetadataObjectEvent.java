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
import org.apache.gravitino.utils.MetadataObjectUtil;

/**
 * Represents an event that is triggered upon successfully listing detailed tag information for a
 * metadata object.
 */
@DeveloperApi
public final class ListTagsInfoForMetadataObjectEvent extends TagEvent implements ListEvent {

  private final int tagCount;

  /**
   * Constructs an instance of {@code ListTagsInfoForMetadataObjectEvent}.
   *
   * @param user The username of the individual who initiated the tag information listing.
   * @param metalake The metalake from which tag information was listed.
   * @param metadataObject The metadata object for which tag information was listed.
   * @param tagCount The number of tags returned by the list operation.
   */
  public ListTagsInfoForMetadataObjectEvent(
      String user, String metalake, MetadataObject metadataObject, int tagCount) {
    super(user, MetadataObjectUtil.toEntityIdent(metalake, metadataObject));
    this.tagCount = tagCount;
  }

  /**
   * Constructs an instance of {@code ListTagsInfoForMetadataObjectEvent} without a result count.
   *
   * @param user The username of the individual who initiated the tag information listing.
   * @param metalake The metalake from which tag information was listed.
   * @param metadataObject The metadata object for which tag information was listed.
   * @deprecated Use {@link #ListTagsInfoForMetadataObjectEvent(String, String, MetadataObject,
   *     int)} instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ListTagsInfoForMetadataObjectEvent(
      String user, String metalake, MetadataObject metadataObject) {
    this(user, metalake, metadataObject, -1);
  }

  /** {@inheritDoc} */
  @Override
  public int resultCount() {
    return tagCount;
  }

  /**
   * Returns the type of operation.
   *
   * @return The operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_TAGS_INFO_FOR_METADATA_OBJECT;
  }
}
