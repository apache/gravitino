/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitions;

import static com.datastrato.gravitino.dto.rel.PartitionUtils.validateFieldExistence;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.truncate;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.rel.expressions.Expression;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public final class TruncatePartitioningDTO implements Partitioning {
  public static TruncatePartitioningDTO of(int width, String[] fieldName) {
    return new TruncatePartitioningDTO(width, fieldName);
  }

  private final int width;
  private final String[] fieldName;

  private TruncatePartitioningDTO(int width, String[] fieldName) {
    this.width = width;
    this.fieldName = fieldName;
  }

  public int width() {
    return width;
  }

  public String[] fieldName() {
    return fieldName;
  }

  @Override
  public String name() {
    return strategy().name().toLowerCase();
  }

  @Override
  public Expression[] arguments() {
    return truncate(width, fieldName).arguments();
  }

  @Override
  public Strategy strategy() {
    return Strategy.TRUNCATE;
  }

  @Override
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    validateFieldExistence(columns, fieldName);
  }
}
