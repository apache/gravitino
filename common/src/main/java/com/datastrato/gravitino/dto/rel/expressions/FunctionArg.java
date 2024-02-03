/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.expressions;

import static com.datastrato.gravitino.dto.rel.PartitionUtils.validateFieldExistence;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.rel.expressions.Expression;
import java.util.Arrays;

/** An argument of a function. */
public interface FunctionArg extends Expression {

  /** Constant for an empty array of function arguments. */
  FunctionArg[] EMPTY_ARGS = new FunctionArg[0];

  /**
   * Arguments type of the function.
   *
   * @return The type of the argument.
   */
  ArgType argType();

  /**
   * Validates the function argument.
   *
   * @param columns The columns of the table.
   * @throws IllegalArgumentException If the function argument is invalid.
   */
  default void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    Arrays.stream(references()).forEach(ref -> validateFieldExistence(columns, ref.fieldName()));
  }

  /** The type of the argument. */
  enum ArgType {
    /** A literal argument. */
    LITERAL,

    /** * A field argument. */
    FIELD,

    /** A function argument. */
    FUNCTION,

    /** An argument that cannot be parsed. */
    UNPARSED
  }
}
