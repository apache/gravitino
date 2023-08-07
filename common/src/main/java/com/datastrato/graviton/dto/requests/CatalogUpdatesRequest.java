/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.requests;

import com.datastrato.graviton.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a request containing multiple catalog updates. */
@Getter
@EqualsAndHashCode
@ToString
public class CatalogUpdatesRequest implements RESTRequest {

  @JsonProperty("updates")
  private final List<CatalogUpdateRequest> updates;

  /**
   * Constructor for CatalogUpdatesRequest.
   *
   * @param requests The list of catalog update requests.
   */
  public CatalogUpdatesRequest(List<CatalogUpdateRequest> requests) {
    this.requests = requests;
  }

  /** Default constructor for CatalogUpdatesRequest. */
  public CatalogUpdatesRequest() {
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
