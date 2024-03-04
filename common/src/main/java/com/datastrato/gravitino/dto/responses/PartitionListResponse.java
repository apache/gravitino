/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.rel.partitions.PartitionDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a response for a list of partitions. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class PartitionListResponse extends BaseResponse {

  @JsonProperty("partitions")
  private final PartitionDTO[] partitions;

  /**
   * Creates a new PartitionListResponse.
   *
   * @param partitions The list of partitions.
   */
  public PartitionListResponse(PartitionDTO[] partitions) {
    super(0);
    this.partitions = partitions;
  }

  /**
   * This is the constructor that is used by Jackson deserializer to create an instance of
   * PartitionListResponse.
   */
  public PartitionListResponse() {
    super();
    this.partitions = null;
  }
}
