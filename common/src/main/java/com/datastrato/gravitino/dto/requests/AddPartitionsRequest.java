/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.dto.rel.partitions.PartitionDTO;
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
public class AddPartitionsRequest implements RESTRequest {

  @JsonProperty("partitions")
  private final PartitionDTO[] partitions;

  /** Default constructor for Jackson. */
  public AddPartitionsRequest() {
    this(null);
  }

  /**
   * Constructor for the request.
   *
   * @param partitions The partitions to add.
   */
  public AddPartitionsRequest(PartitionDTO[] partitions) {
    this.partitions = partitions;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(partitions != null, "partitions must not be null");
    Preconditions.checkArgument(
        partitions.length == 1, "Haven't yet implemented multiple partitions");
  }
}
