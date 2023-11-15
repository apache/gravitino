/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.distributions;

import java.util.Arrays;

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
