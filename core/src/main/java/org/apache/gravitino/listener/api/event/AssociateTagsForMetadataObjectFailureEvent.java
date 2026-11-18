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

import java.util.Arrays;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.MetadataObjectUtil;

/**
 * Represents an event triggered when an attempt to associate tags for a metadata object fails due
 * to an exception.
 */
@DeveloperApi
public class AssociateTagsForMetadataObjectFailureEvent extends TagFailureEvent {
  private final MetadataObject.Type objectType;
  private final String[] tagsToAdd;
  private final String[] tagsToRemove;
  private final TagValue[] tagValuesToAdd;
  private final TagValue[] tagValuesToRemove;

  /**
   * Constructs a new {@code AssociateTagsForMetadataObjectFailureEvent} instance.
   *
   * @param user The user who initiated the operation.
   * @param metalake The metalake name where the metadata object resides.
   * @param metadataObject The metadata object for which tags are being associated.
   * @param tagsToAdd The tags to add.
   * @param tagsToRemove The tags to remove.
   * @param exception The exception encountered during the operation, providing insights into the
   *     reasons behind the failure.
   */
  public AssociateTagsForMetadataObjectFailureEvent(
      String user,
      String metalake,
      MetadataObject metadataObject,
      String[] tagsToAdd,
      String[] tagsToRemove,
      Exception exception) {
    this(user, metalake, metadataObject, tagValues(tagsToAdd), tagValues(tagsToRemove), exception);
  }

  /**
   * Constructs a new {@code AssociateTagsForMetadataObjectFailureEvent} instance.
   *
   * @param user The user who initiated the operation.
   * @param metalake The metalake name where the metadata object resides.
   * @param metadataObject The metadata object for which tags are being associated.
   * @param tagsToAdd The tag values to add.
   * @param tagsToRemove The tag values to remove.
   * @param exception The exception encountered during the operation, providing insights into the
   *     reasons behind the failure.
   */
  public AssociateTagsForMetadataObjectFailureEvent(
      String user,
      String metalake,
      MetadataObject metadataObject,
      TagValue[] tagsToAdd,
      TagValue[] tagsToRemove,
      Exception exception) {
    super(user, MetadataObjectUtil.toEntityIdent(metalake, metadataObject), exception);
    this.objectType = metadataObject.type();
    this.tagsToAdd = tagNames(tagsToAdd);
    this.tagsToRemove = tagNames(tagsToRemove);
    this.tagValuesToAdd = copyTagValues(tagsToAdd);
    this.tagValuesToRemove = copyTagValues(tagsToRemove);
  }

  /**
   * Provides the type of metadata object associated with this event.
   *
   * @return The type of metadata object.
   */
  public MetadataObject.Type objectType() {
    return objectType;
  }

  /**
   * Returns the tags to add.
   *
   * @return The tags to add.
   */
  public String[] tagsToAdd() {
    return tagsToAdd.clone();
  }

  /**
   * Returns the tags to remove.
   *
   * @return The tags to remove.
   */
  public String[] tagsToRemove() {
    return tagsToRemove.clone();
  }

  /**
   * Returns the tag values to add.
   *
   * @return The tag values to add.
   */
  public TagValue[] tagValuesToAdd() {
    return tagValuesToAdd.clone();
  }

  /**
   * Returns the tag values to remove.
   *
   * @return The tag values to remove.
   */
  public TagValue[] tagValuesToRemove() {
    return tagValuesToRemove.clone();
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ASSOCIATE_TAGS_FOR_METADATA_OBJECT;
  }

  private static TagValue[] tagValues(String[] tagNames) {
    if (tagNames == null) {
      return new TagValue[0];
    }

    return Arrays.stream(tagNames).map(TagValue::noValue).toArray(TagValue[]::new);
  }

  private static String[] tagNames(TagValue[] tagValues) {
    if (tagValues == null) {
      return new String[0];
    }

    return Arrays.stream(tagValues).map(TagValue::name).toArray(String[]::new);
  }

  private static TagValue[] copyTagValues(TagValue[] tagValues) {
    return tagValues == null ? new TagValue[0] : tagValues.clone();
  }
}
