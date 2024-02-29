/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.utils;

import java.util.UUID;

public class RandomNameUtils {
  private RandomNameUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static String genRandomName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
  }
}
