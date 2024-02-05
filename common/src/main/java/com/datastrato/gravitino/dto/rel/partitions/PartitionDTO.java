/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitions;

import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/** Represents a Partition Data Transfer Object (DTO) that implements the Partition interface. */
@JsonSerialize(using = JsonUtils.PartitionDTOSerializer.class)
@JsonDeserialize(using = JsonUtils.PartitionDTODeserializer.class)
public interface PartitionDTO extends Partition {

  /** @return The type of the partition. */
  Type type();

  /** Type of the partition. */
  enum Type {
    /** The range partition type. */
    RANGE,

    /** The list partition type. */
    LIST,

    /** The identity partition type. */
    IDENTITY
  }
}
