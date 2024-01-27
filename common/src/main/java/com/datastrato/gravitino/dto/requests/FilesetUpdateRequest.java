/*
 * Copyright 2023 Datastrato Pvt Ltd.
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
  FilesetChange filesetChange();

  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class RenameFilesetRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("newName")
    private final String newName;

    @Override
    public FilesetChange filesetChange() {
      return FilesetChange.rename(newName);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newName), "\"newName\" field is required and cannot be empty");
    }
  }

  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class UpdateFilesetCommentRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("newComment")
    private final String newComment;

    @Override
    public FilesetChange filesetChange() {
      return FilesetChange.updateComment(newComment);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newComment),
          "\"newComment\" field is required and cannot be empty");
    }
  }

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

    @Override
    public FilesetChange filesetChange() {
      return FilesetChange.setProperty(property, value);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(value != null, "\"value\" field is required and cannot be null");
    }
  }

  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class RemoveFilesetPropertiesRequest implements FilesetUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @Override
    public FilesetChange filesetChange() {
      return FilesetChange.removeProperty(property);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
    }
  }
}
