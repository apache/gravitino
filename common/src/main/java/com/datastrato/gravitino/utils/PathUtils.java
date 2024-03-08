/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.utils;

import com.google.common.base.Preconditions;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.lang3.StringUtils;

/** Provides Helper methods to process paths. */
public class PathUtils {

  /**
   * Helper methods to transform path to absolutePath, if path is relative, it's supposed to under
   * gravitinoHome directory.
   *
   * @param path The path to transform.
   * @param gravitinoHome Gravitino home directory.
   * @return The absolute path.
   */
  public static String getAbsolutePath(String path, String gravitinoHome) {
    Path p = Paths.get(path);
    if (p.isAbsolute()) {
      return p.toString();
    }

    Preconditions.checkArgument(
        StringUtils.isNotBlank(gravitinoHome), "GRAVITINO_HOME should not empty");
    Path newPath = Paths.get(gravitinoHome, path);
    return newPath.toString();
  }

  private PathUtils() {}
}
