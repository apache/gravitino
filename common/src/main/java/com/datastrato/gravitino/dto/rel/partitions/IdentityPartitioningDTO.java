/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitions;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.ArrayUtils;

@EqualsAndHashCode(callSuper = true)
public final class IdentityPartitioningDTO extends Partitioning.SingleFieldPartitioning {

  public static IdentityPartitioningDTO of(String... fieldName) {
    return new IdentityPartitioningDTO(fieldName);
  }

  private IdentityPartitioningDTO(String[] fieldName) {
    Preconditions.checkArgument(
        ArrayUtils.isNotEmpty(fieldName), "fieldName cannot be null or empty");
    this.fieldName = fieldName;
  }

  @Override
  public Strategy strategy() {
    return Strategy.IDENTITY;
  }
}
