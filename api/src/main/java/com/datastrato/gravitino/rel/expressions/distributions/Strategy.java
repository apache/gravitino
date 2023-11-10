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

  public static Strategy fromString(String strategy) {
    try {
      return valueOf(strategy.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid distribution strategy: "
              + strategy
              + ". Valid values are: "
              + Arrays.toString(Strategy.values()));
    }
  }
}
