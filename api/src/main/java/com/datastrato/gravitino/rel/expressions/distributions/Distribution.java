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

  /**
   * Returns the number of buckets/distribution. For example, if the distribution strategy is HASH
   * and the number is 10, then the data is distributed across 10 buckets.
   */
  int number();

  /** Returns the expressions passed to the distribution function. */
  Expression[] expressions();

  @Override
  default Expression[] children() {
    return expressions();
  }
}
