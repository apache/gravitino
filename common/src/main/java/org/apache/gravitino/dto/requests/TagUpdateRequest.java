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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rest.RESTRequest;
import org.apache.gravitino.tag.TagChange;

/** Represents a request to update a tag. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = TagUpdateRequest.RenameTagRequest.class, name = "rename"),
  @JsonSubTypes.Type(
      value = TagUpdateRequest.UpdateTagCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(value = TagUpdateRequest.SetTagPropertyRequest.class, name = "setProperty"),
  @JsonSubTypes.Type(
      value = TagUpdateRequest.RemoveTagPropertyRequest.class,
      name = "removeProperty")
})
public interface TagUpdateRequest extends RESTRequest {

  /**
   * Returns the tag change.
   *
   * @return the tag change.
   */
  TagChange tagChange();

  /** The tag update request for renaming a tag. */
  @EqualsAndHashCode
  @ToString
  class RenameTagRequest implements TagUpdateRequest {

    @Getter
    @JsonProperty("newName")
    private final String newName;

    /**
     * Creates a new RenameTagRequest.
     *
     * @param newName The new name of the tag.
     */
    public RenameTagRequest(String newName) {
      this.newName = newName;
    }

    /** This is the constructor that is used by Jackson deserializer */
    public RenameTagRequest() {
      this.newName = null;
    }

    @Override
    public TagChange tagChange() {
      return TagChange.rename(newName);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(StringUtils.isNotBlank(newName), "\"newName\" must not be blank");
    }
  }

  /** The tag update request for updating a tag comment. */
  @EqualsAndHashCode
  @ToString
  class UpdateTagCommentRequest implements TagUpdateRequest {

    @Getter
    @JsonProperty("newComment")
    private final String newComment;

    /**
     * Creates a new UpdateTagCommentRequest.
     *
     * @param newComment The new comment of the tag.
     */
    public UpdateTagCommentRequest(String newComment) {
      this.newComment = newComment;
    }

    /** This is the constructor that is used by Jackson deserializer */
    public UpdateTagCommentRequest() {
      this.newComment = null;
    }

    @Override
    public TagChange tagChange() {
      return TagChange.updateComment(newComment);
    }

    /** Validates the fields of the request. Always pass. */
    @Override
    public void validate() throws IllegalArgumentException {}
  }

  /** The tag update request for setting a tag property. */
  @EqualsAndHashCode
  @ToString
  class SetTagPropertyRequest implements TagUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @Getter
    @JsonProperty("value")
    private final String value;

    /**
     * Creates a new SetTagPropertyRequest.
     *
     * @param property The property to set.
     * @param value The value of the property.
     */
    public SetTagPropertyRequest(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /** This is the constructor that is used by Jackson deserializer */
    public SetTagPropertyRequest() {
      this.property = null;
      this.value = null;
    }

    @Override
    public TagChange tagChange() {
      return TagChange.setProperty(property, value);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" must not be blank");
      Preconditions.checkArgument(StringUtils.isNotBlank(value), "\"value\" must not be blank");
    }
  }

  /** The tag update request for removing a tag property. */
  @EqualsAndHashCode
  @ToString
  class RemoveTagPropertyRequest implements TagUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    /**
     * Creates a new RemoveTagPropertyRequest.
     *
     * @param property The property to remove.
     */
    public RemoveTagPropertyRequest(String property) {
      this.property = property;
    }

    /** This is the constructor that is used by Jackson deserializer */
    public RemoveTagPropertyRequest() {
      this.property = null;
    }

    @Override
    public TagChange tagChange() {
      return TagChange.removeProperty(property);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" must not be blank");
    }
  }
}
