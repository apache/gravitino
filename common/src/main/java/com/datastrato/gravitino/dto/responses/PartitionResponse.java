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

/** Represents a response for a partition. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class PartitionResponse extends BaseResponse {

  @JsonProperty("partition")
  private final PartitionDTO partition;

  /**
   * Creates a new PartitionResponse.
   *
   * @param partition The partition.
   */
  public PartitionResponse(PartitionDTO partition) {
    super(0);
    this.partition = partition;
  }

  /** This is the constructor that is used by Jackson deserializer */
  public PartitionResponse() {
    super();
    this.partition = null;
  }
}
