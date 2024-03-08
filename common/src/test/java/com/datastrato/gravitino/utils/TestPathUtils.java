/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.utils;

import java.nio.file.Paths;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPathUtils {

  @Test
  void testGetAbstractPath() {
    String gravitinoHome = Paths.get("home", "gravitino").toString();
    String currentDirectory = Paths.get("").toAbsolutePath().toString();

    String path = PathUtils.getAbsolutePath(currentDirectory, gravitinoHome);
    Assertions.assertEquals(currentDirectory, path);

    String relativePath = "abc";
    path = PathUtils.getAbsolutePath(relativePath, gravitinoHome);
    String expectedPath = Paths.get(gravitinoHome, relativePath).toString();
    Assertions.assertEquals(expectedPath, path);
  }
}
