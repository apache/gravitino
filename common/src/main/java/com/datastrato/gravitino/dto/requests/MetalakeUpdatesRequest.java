/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a request containing multiple Metalake updates. */
@Getter
@EqualsAndHashCode
@ToString
public class MetalakeUpdatesRequest implements RESTRequest {

  @JsonProperty("updates")
  private final List<MetalakeUpdateRequest> updates;

  /**
   * Constructor for MetalakeUpdatesRequest.
   *
   * @param updates The list of Metalake update requests.
   */
  public MetalakeUpdatesRequest(List<MetalakeUpdateRequest> updates) {
    this.updates = updates;
  }

  /** Default constructor for MetalakeUpdatesRequest. */
  public MetalakeUpdatesRequest() {
    this(null);
  }

  /**
   * Validates each request in the list.
   *
   * @throws IllegalArgumentException if validation of any request fails.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    updates.forEach(RESTRequest::validate);
  }
}
