/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;

/** Represents a request to create a topic. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class TopicCreateRequest implements RESTRequest {
  @JsonProperty("name")
  private final String name;

  @Nullable
  @JsonProperty("comment")
  private final String comment;

  @Nullable
  @JsonProperty("properties")
  private final Map<String, String> properties;

  /** Default constructor for Jackson deserialization. */
  public TopicCreateRequest() {
    this(null, null, null);
  }

  /**
   * Creates a topic create request.
   *
   * @param name The name of the topic.
   * @param comment The comment of the topic.
   * @param properties The properties of the topic.
   */
  public TopicCreateRequest(
      String name, @Nullable String comment, @Nullable Map<String, String> properties) {
    this.name = name;
    this.comment = comment;
    this.properties = properties;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
  }
}
