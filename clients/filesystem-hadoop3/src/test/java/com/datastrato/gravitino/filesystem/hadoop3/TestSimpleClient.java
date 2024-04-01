/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSimpleClient extends TestGvfsBase {
  @BeforeAll
  public static void setup() {
    TestGvfsBase.setup();
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE);
  }

  @Test
  public void testConfiguration() throws IOException {
    try (FileSystem fs = managedFilesetPath.getFileSystem(conf)) {
      assertNotNull(
          fs.getConf()
              .get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY));
      assertEquals(
          GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE,
          fs.getConf()
              .get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY));
    }
  }
}
