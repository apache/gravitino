/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/** Represents a request to create a schema. */
@Getter
@EqualsAndHashCode
@ToString
public class SchemaCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @Nullable
  @JsonProperty("comment")
  private final String comment;

  @Nullable
  @JsonProperty("properties")
  private final Map<String, String> properties;

  /** Default constructor for Jackson deserialization. */
  public SchemaCreateRequest() {
    this(null, null, null);
  }

  /**
   * Creates a new SchemaCreateRequest.
   *
   * @param name The name of the schema.
   * @param comment The comment of the schema.
   * @param properties The properties of the schema.
   */
  public SchemaCreateRequest(String name, String comment, Map<String, String> properties) {
    this.name = name;
    this.comment = comment;
    this.properties = properties;
  }

  /**
   * Validates the request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
  }
}
