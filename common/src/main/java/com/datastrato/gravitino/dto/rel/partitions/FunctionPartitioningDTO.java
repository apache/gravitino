/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitions;

import static com.datastrato.gravitino.dto.rel.PartitionUtils.validateFieldExistence;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.expressions.FunctionArg;
import com.datastrato.gravitino.rel.expressions.Expression;
import java.util.Arrays;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public final class FunctionPartitioningDTO implements Partitioning {

  public static FunctionPartitioningDTO of(String functionName, FunctionArg... args) {
    return new FunctionPartitioningDTO(functionName, args);
  }

  String functionName;
  FunctionArg[] args;

  private FunctionPartitioningDTO(String functionName, FunctionArg[] args) {
    this.functionName = functionName;
    this.args = args;
  }

  public String functionName() {
    return functionName;
  }

  public FunctionArg[] args() {
    return args;
  }

  @Override
  public String name() {
    return functionName;
  }

  @Override
  public Expression[] arguments() {
    return args;
  }

  @Override
  public Strategy strategy() {
    return Strategy.FUNCTION;
  }

  @Override
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    Arrays.stream(references()).forEach(ref -> validateFieldExistence(columns, ref.fieldName()));
  }
}
