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

/** Represents an event triggered before associating tags with a specific metadata object. */
@DeveloperApi
public class AssociateTagsForMetadataObjectPreEvent extends TagPreEvent {
  private final String[] tagsToAdd;
  private final String[] tagsToRemove;

  /**
   * Constructs the pre-event with user, metalake, metadata object, tags to add, and tags to remove.
   *
   * @param user The user initiating the operation.
   * @param metalake The metalake environment name.
   * @param metadataObject The metadata object for which tags will be associated.
   * @param tagsToAdd Tags to be added to the metadata object.
   * @param tagsToRemove Tags to be removed from the metadata object.
   */
  public AssociateTagsForMetadataObjectPreEvent(
      String user,
      String metalake,
      MetadataObject metadataObject,
      String[] tagsToAdd,
      String[] tagsToRemove) {
    super(user, MetadataObjectUtil.toEntityIdent(metalake, metadataObject));
    this.tagsToAdd = tagsToAdd;
    this.tagsToRemove = tagsToRemove;
  }

  /**
   * Returns the tags to be added.
   *
   * @return Array of tag names to be added.
   */
  public String[] tagsToAdd() {
    return tagsToAdd;
  }

  /**
   * Returns the tags to be removed.
   *
   * @return Array of tag names to be removed.
   */
  public String[] tagsToRemove() {
    return tagsToRemove;
  }

  /**
   * Returns the operation type for this event.
   *
   * @return The operation type, which is ASSOCIATE_TAGS_FOR_METADATA_OBJECT.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ASSOCIATE_TAGS_FOR_METADATA_OBJECT;
  }
}
