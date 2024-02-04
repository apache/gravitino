/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents a response containing a list of partition names. */
@EqualsAndHashCode(callSuper = true)
@ToString
public class PartitionNameListResponse extends BaseResponse {

  @JsonProperty("names")
  private final String[] partitionNames;

  /**
   * Constructor for PartitionNameListResponse.
   *
   * @param partitionNames The array of partition names.
   */
  public PartitionNameListResponse(String[] partitionNames) {
    super(0);
    this.partitionNames = partitionNames;
  }

  /** Default constructor for PartitionNameListResponse. (Used for Jackson deserialization.) */
  public PartitionNameListResponse() {
    super();
    this.partitionNames = null;
  }

  /** @return The array of partition names. */
  public String[] partitionNames() {
    return partitionNames;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if partition names are not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    if (partitionNames == null) {
      throw new IllegalArgumentException("partition names must not be null");
    }
  }
}
