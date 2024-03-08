/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.distributions;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.rel.expressions.Expression;
import java.util.Arrays;

/** An interface that defines how data is distributed across partitions. */
@Evolving
public interface Distribution extends Expression {

  /** @return the distribution strategy name. */
  Strategy strategy();

  /**
   * @return The number of buckets/distribution. For example, if the distribution strategy is HASH
   *     and the number is 10, then the data is distributed across 10 buckets.
   */
  int number();

  /** @return The expressions passed to the distribution function. */
  Expression[] expressions();

  @Override
  default Expression[] children() {
    return expressions();
  }

  /**
   * Indicates whether some other object is "equal to" this one.
   *
   * @param distribution The reference distribution object with which to compare.
   * @return returns true if this object is the same as the obj argument; false otherwise.
   */
  default boolean equals(Distribution distribution) {
    if (distribution == null) {
      return false;
    }

    return strategy().equals(distribution.strategy())
        && number() == distribution.number()
        && Arrays.equals(expressions(), distribution.expressions());
  }
}
