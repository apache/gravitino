/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitioning;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.ArrayUtils;

/** Data transfer object representing day partitioning. */
@EqualsAndHashCode(callSuper = true)
public final class DayPartitioningDTO extends Partitioning.SingleFieldPartitioning {

  /**
   * Creates a new instance of {@link DayPartitioningDTO}.
   *
   * @param fieldName The field name.
   * @return The new instance.
   */
  public static DayPartitioningDTO of(String... fieldName) {
    return new DayPartitioningDTO(fieldName);
  }

  private DayPartitioningDTO(String[] fieldName) {
    Preconditions.checkArgument(
        ArrayUtils.isNotEmpty(fieldName), "fieldName cannot be null or empty");
    this.fieldName = fieldName;
  }

  /** @return The strategy of the partitioning. */
  @Override
  public Strategy strategy() {
    return Strategy.DAY;
  }
}
