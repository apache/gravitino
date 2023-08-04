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

@Getter
@EqualsAndHashCode
@ToString
public class CatalogUpdatesRequest implements RESTRequest {

  @JsonProperty("updates")
  private final List<CatalogUpdateRequest> updates;

  public CatalogUpdatesRequest(List<CatalogUpdateRequest> updates) {
    this.updates = updates;
  }

  public CatalogUpdatesRequest() {
    this(null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    updates.forEach(RESTRequest::validate);
  }
}
