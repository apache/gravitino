/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.expressions;

import static com.datastrato.gravitino.dto.rel.PartitionUtils.validateFieldExistence;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.rel.expressions.Expression;
import java.util.Arrays;

public interface FunctionArg extends Expression {

  FunctionArg[] EMPTY_ARGS = new FunctionArg[0];

  ArgType argType();

  default void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    Arrays.stream(references()).forEach(ref -> validateFieldExistence(columns, ref.fieldName()));
  }

  enum ArgType {
    LITERAL,
    FIELD,
    FUNCTION,
    UNPARSED
  }
}
