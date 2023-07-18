/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton.dto.requests;

import com.datastrato.graviton.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class MetalakeCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private String name;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  // Only for Jackson deserialization
  public MetalakeCreateRequest() {
    this(null, null, null);
  }

  public MetalakeCreateRequest(String name, String comment, Map<String, String> properties) {
    super();

    this.name = Optional.ofNullable(name).map(String::trim).orElse(null);
    this.comment = Optional.ofNullable(comment).map(String::trim).orElse(null);
    this.properties = properties;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        name != null && !name.isEmpty(), "\"name\" field is required and cannot be empty");
  }
}
