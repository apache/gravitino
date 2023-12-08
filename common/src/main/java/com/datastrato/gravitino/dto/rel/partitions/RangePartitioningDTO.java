/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitions;

import static com.datastrato.gravitino.dto.rel.PartitionUtils.validateFieldExistence;
import static com.datastrato.gravitino.rel.expressions.NamedReference.field;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.rel.expressions.Expression;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public final class RangePartitioningDTO implements Partitioning {
  public static RangePartitioningDTO of(String[] fieldName) {
    return new RangePartitioningDTO(fieldName);
  }

  private final String[] fieldName;

  private RangePartitioningDTO(String[] fieldName) {
    this.fieldName = fieldName;
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
    return new Expression[] {field(fieldName)};
  }

  @Override
  public Strategy strategy() {
    return Strategy.RANGE;
  }

  @Override
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    validateFieldExistence(columns, fieldName);
  }

  public static class Builder {
    private String[] fieldName;

    public Builder withFieldName(String[] fieldName) {
      this.fieldName = fieldName;
      return this;
    }

    public RangePartitioningDTO build() {
      return new RangePartitioningDTO(fieldName);
    }
  }
}
