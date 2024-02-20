/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.rest.RESTRequest;
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

/** Request to update a fileset. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = FilesetUpdateRequest.RenameFilesetRequest.class, name = "rename"),
  @JsonSubTypes.Type(
      value = FilesetUpdateRequest.UpdateFilesetCommentRequest.class,
      name = "updateComment"),
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

    /** @return The fileset change. */
    @Override
    public FilesetChange filesetChange() {
      return FilesetChange.updateComment(newComment);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException if the request is invalid.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newComment),
          "\"newComment\" field is required and cannot be empty");
    }
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

    /** @return The fileset change. */
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

    /** @return The fileset change. */
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
}
