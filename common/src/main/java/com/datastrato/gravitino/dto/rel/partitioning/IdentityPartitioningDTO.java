/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitioning;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.ArrayUtils;

/** Represents the identity partitioning. */
@EqualsAndHashCode(callSuper = true)
public final class IdentityPartitioningDTO extends Partitioning.SingleFieldPartitioning {

  /**
   * Creates a new IdentityPartitioningDTO.
   *
   * @param fieldName The name of the field to partition.
   * @return The new IdentityPartitioningDTO.
   */
  public static IdentityPartitioningDTO of(String... fieldName) {
    return new IdentityPartitioningDTO(fieldName);
  }

  private IdentityPartitioningDTO(String[] fieldName) {
    Preconditions.checkArgument(
        ArrayUtils.isNotEmpty(fieldName), "fieldName cannot be null or empty");
    this.fieldName = fieldName;
  }

  /** @return The strategy of the partitioning. */
  @Override
  public Strategy strategy() {
    return Strategy.IDENTITY;
  }
}
