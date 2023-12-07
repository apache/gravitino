/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class SchemaUpdatesRequest implements RESTRequest {

  @JsonProperty("updates")
  private final List<SchemaUpdateRequest> updates;

  public SchemaUpdatesRequest(List<SchemaUpdateRequest> updates) {
    this.updates = updates;
  }

  public SchemaUpdatesRequest() {
    this(null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    updates.forEach(RESTRequest::validate);
  }
}
