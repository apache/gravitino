/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.transforms;

import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import lombok.EqualsAndHashCode;

public interface Transform extends Expression {
  /** Returns the transform function name. */
  String name();

  /** Returns the arguments passed to the transform function. */
  Expression[] arguments();

  /**
   * Returns the preassigned partitions in the partitioning. Currently, only ListTransform and
   * RangeTransform need to deal with assignments
   */
  default Expression[] assignments() {
    return Expression.EMPTY_EXPRESSION;
  }

  @Override
  default Expression[] children() {
    return arguments();
  }

  @EqualsAndHashCode
  abstract class SingleFieldTransform implements Transform {
    NamedReference ref;

    public String[] fieldName() {
      return ref.fieldName();
    }

    @Override
    public NamedReference[] references() {
      return new NamedReference[] {ref};
    }

    @Override
    public Expression[] arguments() {
      return new Expression[] {ref};
    }
  }
}
