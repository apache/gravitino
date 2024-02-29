/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.utils;

import java.util.UUID;

/** Tools to generate random values. */
public class RandomNameUtils {
  private RandomNameUtils() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Generate a random string with the prefix.
   *
   * @param prefix Prefix of the random value.
   * @return A random string value.
   */
  public static String genRandomName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
  }
}
