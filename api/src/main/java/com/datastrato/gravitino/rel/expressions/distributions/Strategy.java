/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.distributions;

import com.datastrato.gravitino.annotation.Evolving;
import java.util.Arrays;

/**
 * An enum that defines the distribution strategy.
 *
 * <p>The following strategies are supported:
 *
 * <ul>
 *   <li>Hash: Uses the hash value of the expression to distribute data.
 *   <li>Range: Uses the range of the expression specified to distribute data.
 *   <li>Even: Distributes data evenly across partitions.
 * </ul>
 */
@Evolving
public enum Strategy {
  /**
   * No distribution strategy. This is the default strategy. Will depend on the allocation strategy
   * of the underlying system.
   */
  NONE,

  /** Uses the hash value of the expression to distribute data. */
  HASH,

  /**
   * Uses the range of the expression specified to distribute data. The range is specified using the
   * rangeStart and rangeEnd properties.
   */
  RANGE,

  /** Distributes data evenly across partitions. */
  EVEN;

  /**
   * Get the distribution strategy by name.
   *
   * @param name The name of the distribution strategy.
   * @return The distribution strategy.
   */
  public static Strategy getByName(String name) {
    for (Strategy strategy : Strategy.values()) {
      if (strategy.name().equalsIgnoreCase(name)) {
        return strategy;
      }
    }
    throw new IllegalArgumentException(
        "Invalid distribution strategy: "
            + name
            + ". Valid values are: "
            + Arrays.toString(Strategy.values()));
  }
}
