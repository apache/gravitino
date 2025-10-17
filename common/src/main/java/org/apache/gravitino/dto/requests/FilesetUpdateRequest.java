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
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.rest.RESTRequest;

/** Request to update a fileset. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = FilesetUpdateRequest.RenameFilesetRequest.class, name = "rename"),
  @JsonSubTypes.Type(
      value = FilesetUpdateRequest.UpdateFilesetCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = FilesetUpdateRequest.RemoveFilesetCommentRequest.class,
      name = "removeComment"),
  @JsonSubTypes.Type(
      value = FilesetUpdateRequest.SetFilesetPropertiesRequest.class,
      name = "setProperty"),
  @JsonSubTypes.Type(
      value = FilesetUpdateRequest.RemoveFilesetPropertiesRequest.class,
      name = "removeProperty")
})
public interface FilesetUpdateRequest extends RESTRequest {

  /**
   * Returns the fileset change.
   *
   * @return the fileset change.
   */
  FilesetChange filesetChange();

  /** The fileset update request for renaming a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class RenameFilesetRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("newName")
    private final String newName;

    /**
     * Returns the fileset change.
     *
     * @return the fileset change.
     */
    @Override
    public FilesetChange filesetChange() {
      return FilesetChange.rename(newName);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newName), "\"newName\" field is required and cannot be empty");
    }
  }

  /** The fileset update request for updating the comment of a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class UpdateFilesetCommentRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("newComment")
    private final String newComment;

    /**
     * @return The fileset change.
     */
    @Override
    public FilesetChange filesetChange() {
      return FilesetChange.updateComment(newComment);
    }

    /** Validates the fields of the request. Always pass. */
    @Override
    public void validate() throws IllegalArgumentException {}
  }

  /** The fileset update request for setting the properties of a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class SetFilesetPropertiesRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @Getter
    @JsonProperty("value")
    private final String value;

    /**
     * @return The fileset change.
     */
    @Override
    public FilesetChange filesetChange() {
      return FilesetChange.setProperty(property, value);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(value != null, "\"value\" field is required and cannot be null");
    }
  }

  /** The fileset update request for removing the properties of a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class RemoveFilesetPropertiesRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    /**
     * @return The fileset change.
     */
    @Override
    public FilesetChange filesetChange() {
      return FilesetChange.removeProperty(property);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
    }
  }

  /** The fileset update request for removing the comment of a fileset. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @ToString
  class RemoveFilesetCommentRequest implements FilesetUpdateRequest {

    /**
     * @return The fileset change.
     */
    @Override
    public FilesetChange filesetChange() {
      return FilesetChange.updateComment(null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {}
  }
}
