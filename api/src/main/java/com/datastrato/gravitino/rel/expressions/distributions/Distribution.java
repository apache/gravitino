/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.distributions;

import com.datastrato.gravitino.rel.expressions.Expression;

/** An interface that defines how data is distributed across partitions. */
public interface Distribution extends Expression {
  /** Returns the distribution strategy name. */
  Strategy strategy();

  int number();

  /** Returns the expressions passed to the distribution function. */
  Expression[] expressions();

  @Override
  default Expression[] children() {
    return expressions();
  }
}
