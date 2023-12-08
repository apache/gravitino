/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitions;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;

public final class MonthPartitioningDTO extends Partitioning.SingleFieldPartitioning {
  public static MonthPartitioningDTO of(String[] fieldName) {
    return new MonthPartitioningDTO(fieldName);
  }

  private MonthPartitioningDTO(String[] fieldName) {
    Preconditions.checkArgument(
        ArrayUtils.isNotEmpty(fieldName), "fieldName cannot be null or empty");
    this.fieldName = fieldName;
  }

  @Override
  public Strategy strategy() {
    return Strategy.MONTH;
  }
}
