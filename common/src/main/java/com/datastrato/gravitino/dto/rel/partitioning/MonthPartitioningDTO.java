/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitioning;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Represents a MonthPartitioning Data Transfer Object (DTO) that extends the Partitioning
 * interface.
 */
public final class MonthPartitioningDTO extends Partitioning.SingleFieldPartitioning {

  /**
   * Creates a new MonthPartitioningDTO.
   *
   * @param fieldName The name of the field to partition.
   * @return The new MonthPartitioningDTO.
   */
  public static MonthPartitioningDTO of(String[] fieldName) {
    return new MonthPartitioningDTO(fieldName);
  }

  private MonthPartitioningDTO(String[] fieldName) {
    Preconditions.checkArgument(
        ArrayUtils.isNotEmpty(fieldName), "fieldName cannot be null or empty");
    this.fieldName = fieldName;
  }

  /** @return The strategy of the partitioning. */
  @Override
  public Strategy strategy() {
    return Strategy.MONTH;
  }
}
