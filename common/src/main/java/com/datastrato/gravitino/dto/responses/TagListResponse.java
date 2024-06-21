/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.tag.TagDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a response for a list of tags. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class TagListResponse extends BaseResponse {

  @JsonProperty("tags")
  private final TagDTO[] tags;

  /**
   * Creates a new TagListResponse.
   *
   * @param tags The list of tags.
   */
  public TagListResponse(TagDTO[] tags) {
    super(0);
    this.tags = tags;
  }

  /**
   * This is the constructor that is used by Jackson deserializer to create an instance of
   * TagListResponse.
   */
  public TagListResponse() {
    super();
    this.tags = null;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(tags != null, "\"tags\" must not be null");
  }
}
