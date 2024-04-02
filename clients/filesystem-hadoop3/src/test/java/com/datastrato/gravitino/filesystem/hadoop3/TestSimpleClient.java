/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop3;

import org.junit.jupiter.api.BeforeAll;

public class TestSimpleClient extends TestGvfsBase {
  @BeforeAll
  public static void setup() {
    TestGvfsBase.setup();
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE);
  }
}
