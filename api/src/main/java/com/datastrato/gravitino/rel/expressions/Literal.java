/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions;

import io.substrait.type.Type;
import lombok.EqualsAndHashCode;

public interface Literal<T> extends Expression {
  /** Returns the literal value. */
  T value();

  /** Returns the data type of the literal. */
  Type dataType();

  @Override
  default Expression[] children() {
    return EMPTY_EXPRESSION;
  }

  static <T> LiteralValue<T> of(T value, Type dataType) {
    return new LiteralValue<>(value, dataType);
  }

  @EqualsAndHashCode
  final class LiteralValue<T> implements Literal<T> {
    private final T value;
    private final Type dataType;

    private LiteralValue(T value, Type dataType) {
      this.value = value;
      this.dataType = dataType;
    }

    @Override
    public T value() {
      return value;
    }

    @Override
    public Type dataType() {
      return dataType;
    }
  }
}
