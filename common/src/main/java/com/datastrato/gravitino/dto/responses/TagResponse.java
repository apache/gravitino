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

/** Represents a response for a tag. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class TagResponse extends BaseResponse {

  @JsonProperty("tag")
  private final TagDTO tag;

  /**
   * Creates a new TagResponse.
   *
   * @param tag The tag.
   */
  public TagResponse(TagDTO tag) {
    super(0);
    this.tag = tag;
  }

  /**
   * This is the constructor that is used by Jackson deserializer to create an instance of
   * TagResponse.
   */
  public TagResponse() {
    super();
    this.tag = null;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(tag != null, "tag must not be null");
  }
}
