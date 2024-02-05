/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitioning;

import static com.datastrato.gravitino.dto.rel.PartitionUtils.validateFieldExistence;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.truncate;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.rel.expressions.Expression;
import lombok.EqualsAndHashCode;

/** Represents the truncate partitioning. */
@EqualsAndHashCode
public final class TruncatePartitioningDTO implements Partitioning {

  /**
   * Constructs a truncate partitioning.
   *
   * @param width The width of the truncate partitioning.
   * @param fieldName The name of the field to partition.
   * @return The truncate partitioning.
   */
  public static TruncatePartitioningDTO of(int width, String[] fieldName) {
    return new TruncatePartitioningDTO(width, fieldName);
  }

  private final int width;
  private final String[] fieldName;

  private TruncatePartitioningDTO(int width, String[] fieldName) {
    this.width = width;
    this.fieldName = fieldName;
  }

  /** @return The width of the partitioning. */
  public int width() {
    return width;
  }

  /** @return The name of the field to partition. */
  public String[] fieldName() {
    return fieldName;
  }

  /** @return The name of the partitioning strategy. */
  @Override
  public String name() {
    return strategy().name().toLowerCase();
  }

  /** @return The arguments of the partitioning. */
  @Override
  public Expression[] arguments() {
    return truncate(width, fieldName).arguments();
  }

  /** @return The strategy of the partitioning. */
  @Override
  public Strategy strategy() {
    return Strategy.TRUNCATE;
  }

  /**
   * Validates the partitioning columns.
   *
   * @param columns The columns to be validated.
   * @throws IllegalArgumentException If the columns are invalid, this exception is thrown.
   */
  @Override
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    validateFieldExistence(columns, fieldName);
  }
}
