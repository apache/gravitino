/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitioning;

import static com.datastrato.gravitino.dto.rel.PartitionUtils.validateFieldExistence;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
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
    return new ListPartitioningDTO(fieldNames);
  }

  private final String[][] fieldNames;

  private ListPartitioningDTO(String[][] fieldNames) {
    this.fieldNames = fieldNames;
  }

  /** @return The names of the fields to partition. */
  public String[][] fieldNames() {
    return fieldNames;
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

  /** The builder for ListPartitioningDTO. */
  public static class Builder {
    private String[][] fieldNames;

    /**
     * Set the field names for the builder.
     *
     * @param fieldNames The names of the fields to partition.
     * @return The builder.
     */
    public Builder withFieldNames(String[][] fieldNames) {
      this.fieldNames = fieldNames;
      return this;
    }

    /**
     * Builds the ListPartitioningDTO.
     *
     * @return The ListPartitioningDTO.
     */
    public ListPartitioningDTO build() {
      return new ListPartitioningDTO(fieldNames);
    }
  }
}
