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

import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event that is triggered when listing metadata objects for multiple tags fails. */
@DeveloperApi
public final class ListMetadataObjectsForTagsFailureEvent extends TagFailureEvent {
  private final String[] tagNames;

  /**
   * Constructs an instance of {@code ListMetadataObjectsForTagsFailureEvent}.
   *
   * @param user The username of the individual who initiated the metadata objects listing.
   * @param metalake The metalake from which metadata objects were being listed.
   * @param tagNames The names of the tags for which metadata objects listing failed.
   * @param failure The exception that caused the failure.
   */
  public ListMetadataObjectsForTagsFailureEvent(
      String user, String metalake, String[] tagNames, Exception failure) {
    super(
        user,
        NameIdentifierUtil.ofTag(metalake, tagNames[0]),
        failure); // Use first tag for identifier
    this.tagNames = tagNames;
  }

  /**
   * Returns the tag names.
   *
   * @return the tag names.
   */
  public String[] tagNames() {
    return tagNames;
  }

  /**
   * Returns the type of operation.
   *
   * @return The operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_METADATA_OBJECTS_FOR_TAGS;
  }
}
