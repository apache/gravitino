/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitioning;

import static com.datastrato.gravitino.dto.rel.PartitionUtils.validateFieldExistence;
import static com.datastrato.gravitino.rel.expressions.NamedReference.field;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.partitions.RangePartitionDTO;
import com.datastrato.gravitino.rel.expressions.Expression;
import lombok.EqualsAndHashCode;

/** Represents the range partitioning. */
@EqualsAndHashCode
public final class RangePartitioningDTO implements Partitioning {

  /**
   * Creates a new RangePartitioningDTO.
   *
   * @param fieldName The name of the field to partition.
   * @return The new RangePartitioningDTO.
   */
  public static RangePartitioningDTO of(String[] fieldName) {
    return of(fieldName, new RangePartitionDTO[0]);
  }

  public static RangePartitioningDTO of(String[] fieldName, RangePartitionDTO[] assignments) {
    return new RangePartitioningDTO(fieldName, assignments);
  }

  private final String[] fieldName;
  private final RangePartitionDTO[] assignments;

  private RangePartitioningDTO(String[] fieldName, RangePartitionDTO[] assignments) {
    this.fieldName = fieldName;
    this.assignments = assignments;
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

  @Override
  public RangePartitionDTO[] assignments() {
    return assignments;
  }

  /** @return The arguments of the partitioning. */
  @Override
  public Expression[] arguments() {
    return new Expression[] {field(fieldName)};
  }

  /** @return The strategy of the partitioning. */
  @Override
  public Strategy strategy() {
    return Strategy.RANGE;
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
