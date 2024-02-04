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

/** Represents a request to update a table. */
@Getter
@EqualsAndHashCode
@ToString
public class TableUpdatesRequest implements RESTRequest {

  @JsonProperty("updates")
  private final List<TableUpdateRequest> updates;

  /**
   * Creates a new TableUpdatesRequest.
   *
   * @param updates The updates to apply to the table.
   */
  public TableUpdatesRequest(List<TableUpdateRequest> updates) {
    this.updates = updates;
  }

  /** This is the constructor that is used by Jackson deserializer */
  public TableUpdatesRequest() {
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
