/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitions;

import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = JsonUtils.PartitionDTOSerializer.class)
@JsonDeserialize(using = JsonUtils.PartitionDTODeserializer.class)
public interface PartitionDTO extends Partition {

  Type type();

  enum Type {
    RANGE,
    LIST,
    IDENTITY
  }
}
