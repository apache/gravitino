/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.requests;

import com.datastrato.graviton.rest.RESTRequest;
import com.datastrato.graviton.util.StringUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a request to create a Metalake. */
@Getter
@EqualsAndHashCode
@ToString
public class MetalakeCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @Nullable
  @JsonProperty("comment")
  private final String comment;

  @Nullable
  @JsonProperty("properties")
  private final Map<String, String> properties;

  /** Default constructor for MetalakeCreateRequest. (Used for Jackson deserialization.) */
  public MetalakeCreateRequest() {
    this(null, null, null);
  }

  /**
   * Constructor for MetalakeCreateRequest.
   *
   * @param name The name of the Metalake.
   * @param comment The comment for the Metalake.
   * @param properties The properties for the Metalake.
   */
  public MetalakeCreateRequest(String name, String comment, Map<String, String> properties) {
    super();

    this.name = Optional.ofNullable(name).map(String::trim).orElse(null);
    this.comment = Optional.ofNullable(comment).map(String::trim).orElse(null);
    this.properties = properties;
  }

  /**
   * Validates the fields of the request.
   *
   * @throws IllegalArgumentException if the name is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
  }
}
