/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

/** Represents a request to update a tag. */
@Getter
@EqualsAndHashCode
@ToString
public class TagUpdatesRequest implements RESTRequest {

  @JsonProperty("updates")
  private final List<TagUpdateRequest> updates;

  /**
   * Creates a new TagUpdatesRequest.
   *
   * @param updates The updates to apply to the tag.
   */
  public TagUpdatesRequest(List<TagUpdateRequest> updates) {
    this.updates = updates;
  }

  /** This is the constructor that is used by Jackson deserializer */
  public TagUpdatesRequest() {
    this(null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(updates != null, "updates must not be null");
    updates.forEach(RESTRequest::validate);
  }
}
