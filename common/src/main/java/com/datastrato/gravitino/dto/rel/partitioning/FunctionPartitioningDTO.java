/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitioning;

import static com.datastrato.gravitino.dto.rel.PartitionUtils.validateFieldExistence;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.expressions.FunctionArg;
import com.datastrato.gravitino.rel.expressions.Expression;
import java.util.Arrays;
import lombok.EqualsAndHashCode;

/** Data transfer object for function partitioning. */
@EqualsAndHashCode
public final class FunctionPartitioningDTO implements Partitioning {

  /**
   * Creates a new function partitioning DTO.
   *
   * @param functionName The name of the function.
   * @param args The arguments of the function.
   * @return The function partitioning DTO.
   */
  public static FunctionPartitioningDTO of(String functionName, FunctionArg... args) {
    return new FunctionPartitioningDTO(functionName, args);
  }

  String functionName;
  FunctionArg[] args;

  private FunctionPartitioningDTO(String functionName, FunctionArg[] args) {
    this.functionName = functionName;
    this.args = args;
  }

  /**
   * Returns the name of the function.
   *
   * @return The name of the function.
   */
  public String functionName() {
    return functionName;
  }

  /**
   * Returns the arguments of the function.
   *
   * @return The arguments of the function.
   */
  public FunctionArg[] args() {
    return args;
  }

  /**
   * Returns the name of the function.
   *
   * @return The name of the function.
   */
  @Override
  public String name() {
    return functionName;
  }

  /**
   * Returns the arguments of the function.
   *
   * @return The arguments of the function.
   */
  @Override
  public Expression[] arguments() {
    return args;
  }

  /**
   * Returns the strategy of the partitioning.
   *
   * @return The strategy of the partitioning.
   */
  @Override
  public Strategy strategy() {
    return Strategy.FUNCTION;
  }

  /**
   * Validates the function partitioning.
   *
   * @param columns The columns to be validated.
   * @throws IllegalArgumentException If the function partitioning is invalid.
   */
  @Override
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    Arrays.stream(references()).forEach(ref -> validateFieldExistence(columns, ref.fieldName()));
  }
}
