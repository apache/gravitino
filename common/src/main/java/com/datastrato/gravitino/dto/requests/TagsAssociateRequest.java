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
import org.apache.commons.lang3.StringUtils;

/** Represents a request to associate tags. */
@Getter
@EqualsAndHashCode
@ToString
public class TagsAssociateRequest implements RESTRequest {

  @JsonProperty("tagsToAdd")
  private final String[] tagsToAdd;

  @JsonProperty("tagsToRemove")
  private final String[] tagsToRemove;

  /**
   * Creates a new TagsAssociateRequest.
   *
   * @param tagsToAdd The tags to add.
   * @param tagsToRemove The tags to remove.
   */
  public TagsAssociateRequest(String[] tagsToAdd, String[] tagsToRemove) {
    this.tagsToAdd = tagsToAdd;
    this.tagsToRemove = tagsToRemove;
  }

  /** This is the constructor that is used by Jackson deserializer */
  public TagsAssociateRequest() {
    this(null, null);
  }

  /**
   * Validates the request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    if (tagsToAdd != null) {
      for (String tag : tagsToAdd) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(tag), "tagsToAdd must not contain null or empty strings");
      }
    }

    if (tagsToRemove != null) {
      for (String tag : tagsToRemove) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(tag), "tagsToRemove must not contain null or empty strings");
      }
    }
  }
}
