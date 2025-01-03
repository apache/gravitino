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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/** Represents an event that is triggered upon successfully listing tags for a metadata object. */
@DeveloperApi
public final class ListTagsForMetadataObjectEvent extends TagEvent {
  private final String metalake;
  private final MetadataObject metadataObject;
  private final String[] tags;

  /**
   * Constructs an instance of {@code ListTagsForMetadataObjectEvent}.
   *
   * @param user The username of the individual who initiated the tag listing.
   * @param metalake The metalake from which tags were listed.
   * @param metadataObject The metadata object for which tags were listed.
   * @param tags An array of tag names representing the tags listed for the metadata object.
   */
  public ListTagsForMetadataObjectEvent(
      String user, String metalake, MetadataObject metadataObject, String[] tags) {
    super(user, NameIdentifier.of(metalake));
    this.metalake = metalake;
    this.metadataObject = metadataObject;
    this.tags = tags;
  }

  /**
   * Provides the metalake associated with this event.
   *
   * @return The metalake from which tags were listed.
   */
  public String metalake() {
    return metalake;
  }

  /**
   * Provides the metadata object associated with this event.
   *
   * @return The {@link MetadataObject} for which tags were listed.
   */
  public MetadataObject metadataObject() {
    return metadataObject;
  }

  /**
   * Provides the tags associated with this event.
   *
   * @return An array of tag names.
   */
  public String[] tags() {
    return tags;
  }

  /**
   * Returns the type of operation.
   *
   * @return The operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_TAGS_FOR_METADATA_OBJECT;
  }
}
