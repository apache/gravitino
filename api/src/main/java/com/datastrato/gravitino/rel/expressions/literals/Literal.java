/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.literals;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.types.Type;

/**
 * Represents a constant literal value in the public expression API.
 *
 * @param <T> the JVM type of value held by the literal
 */
@Evolving
public interface Literal<T> extends Expression {

  /** @return The literal value. */
  T value();

  /** @return The data type of the literal. */
  Type dataType();

  @Override
  default Expression[] children() {
    return EMPTY_EXPRESSION;
  }
}
