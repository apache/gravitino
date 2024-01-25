/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitioning;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;

public final class YearPartitioningDTO extends Partitioning.SingleFieldPartitioning {
  public static YearPartitioningDTO of(String[] fieldName) {
    return new YearPartitioningDTO(fieldName);
  }

  private YearPartitioningDTO(String[] fieldName) {
    Preconditions.checkArgument(
        ArrayUtils.isNotEmpty(fieldName), "fieldName cannot be null or empty");
    this.fieldName = fieldName;
  }

  @Override
  public Strategy strategy() {
    return Strategy.YEAR;
  }
}
