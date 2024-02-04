/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a request to update a schema. */
@Getter
@EqualsAndHashCode
@ToString
public class SchemaUpdatesRequest implements RESTRequest {

  @JsonProperty("updates")
  private final List<SchemaUpdateRequest> updates;

  /**
   * Creates a new SchemaUpdatesRequest.
   *
   * @param updates The updates to apply to the schema.
   */
  public SchemaUpdatesRequest(List<SchemaUpdateRequest> updates) {
    this.updates = updates;
  }

  /** This is the constructor that is used by Jackson deserializer */
  public SchemaUpdatesRequest() {
    this(null);
  }

  /**
   * Validates the request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    updates.forEach(RESTRequest::validate);
  }
}
