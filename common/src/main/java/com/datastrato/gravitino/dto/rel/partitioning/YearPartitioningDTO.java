/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitioning;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;

/** Represents the year partitioning. */
public final class YearPartitioningDTO extends Partitioning.SingleFieldPartitioning {

  /**
   * Creates a new YearPartitioningDTO.
   *
   * @param fieldName The name of the field to partition.
   * @return The new YearPartitioningDTO.
   */
  public static YearPartitioningDTO of(String[] fieldName) {
    return new YearPartitioningDTO(fieldName);
  }

  private YearPartitioningDTO(String[] fieldName) {
    Preconditions.checkArgument(
        ArrayUtils.isNotEmpty(fieldName), "fieldName cannot be null or empty");
    this.fieldName = fieldName;
  }

  /** @return The strategy of the partitioning. */
  @Override
  public Strategy strategy() {
    return Strategy.YEAR;
  }
}
