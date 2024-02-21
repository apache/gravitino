/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitioning;

import static com.datastrato.gravitino.dto.rel.PartitionUtils.validateFieldExistence;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.partitions.ListPartitionDTO;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import java.util.Arrays;
import lombok.EqualsAndHashCode;

/** Data transfer object representing a list partitioning. */
@EqualsAndHashCode
public final class ListPartitioningDTO implements Partitioning {

  /**
   * Creates a new ListPartitioningDTO.
   *
   * @param fieldNames The names of the fields to partition.
   * @return The new ListPartitioningDTO.
   */
  public static ListPartitioningDTO of(String[][] fieldNames) {
    return of(fieldNames, new ListPartitionDTO[0]);
  }

  public static ListPartitioningDTO of(String[][] fieldNames, ListPartitionDTO[] assignments) {
    return new ListPartitioningDTO(fieldNames, assignments);
  }

  private final String[][] fieldNames;
  private final ListPartitionDTO[] assignments;

  private ListPartitioningDTO(String[][] fieldNames, ListPartitionDTO[] assignments) {
    this.fieldNames = fieldNames;
    this.assignments = assignments;
  }

  /** @return The names of the fields to partition. */
  public String[][] fieldNames() {
    return fieldNames;
  }

  @Override
  public ListPartitionDTO[] assignments() {
    return assignments;
  }

  /** @return The strategy of the partitioning. */
  @Override
  public Strategy strategy() {
    return Strategy.LIST;
  }

  /**
   * Validates the partitioning columns.
   *
   * @param columns The columns to be validated.
   * @throws IllegalArgumentException If the columns are invalid, this exception is thrown.
   */
  @Override
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    Arrays.stream(fieldNames).forEach(fieldName -> validateFieldExistence(columns, fieldName));
  }

  /** @return The name of the partitioning strategy. */
  @Override
  public String name() {
    return strategy().name().toLowerCase();
  }

  /** @return The arguments of the partitioning strategy. */
  @Override
  public Expression[] arguments() {
    return Arrays.stream(fieldNames).map(NamedReference::field).toArray(Expression[]::new);
  }
}
