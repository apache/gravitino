/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitions;

import static com.datastrato.gravitino.dto.rel.PartitionUtils.validateFieldExistence;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import java.util.Arrays;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public final class ListPartitioningDTO implements Partitioning {

  public static ListPartitioningDTO of(String[][] fieldNames) {
    return new ListPartitioningDTO(fieldNames);
  }

  private final String[][] fieldNames;

  private ListPartitioningDTO(String[][] fieldNames) {
    this.fieldNames = fieldNames;
  }

  public String[][] fieldNames() {
    return fieldNames;
  }

  @Override
  public Strategy strategy() {
    return Strategy.LIST;
  }

  @Override
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    Arrays.stream(fieldNames).forEach(fieldName -> validateFieldExistence(columns, fieldName));
  }

  @Override
  public String name() {
    return strategy().name().toLowerCase();
  }

  @Override
  public Expression[] arguments() {
    return Arrays.stream(fieldNames).map(NamedReference::field).toArray(Expression[]::new);
  }

  public static class Builder {
    private String[][] fieldNames;

    public Builder withFieldNames(String[][] fieldNames) {
      this.fieldNames = fieldNames;
      return this;
    }

    public ListPartitioningDTO build() {
      return new ListPartitioningDTO(fieldNames);
    }
  }
}
