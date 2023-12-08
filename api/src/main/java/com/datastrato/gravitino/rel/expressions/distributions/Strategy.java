/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.distributions;

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
public enum Strategy {
  HASH,
  RANGE,
  EVEN;

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
