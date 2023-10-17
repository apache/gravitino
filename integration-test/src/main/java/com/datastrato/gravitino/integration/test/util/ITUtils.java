/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.util;

import java.io.File;

public class ITUtils {
  public static String joinDirPath(String... dirs) {
    return String.join(File.separator, dirs);
  }
}
