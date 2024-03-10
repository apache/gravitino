/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;

/** Request to add partitions to a table. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class DropPartitionsRequest implements RESTRequest {

  @JsonProperty("partitionNames")
  private final String[] partitionNames;

  /** Default constructor for Jackson. */
  public DropPartitionsRequest() {
    this(null);
  }

  /**
   * Constructor for the request.
   *
   * @param partitionNames The partitionNames to add.
   */
  public DropPartitionsRequest(String[] partitionNames) {
    this.partitionNames = partitionNames;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(partitionNames != null, "partitions must not be null");
    Preconditions.checkArgument(
        partitionNames.length == 1, "Haven't yet implemented multiple partitions");
  }
}
