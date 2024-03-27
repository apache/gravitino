/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop3;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class TestOauth2Client extends TestGvfsBase {
  @BeforeAll
  public static void setup() {
    Oauth2MockServerBase.setup();
    TestGvfsBase.setup();
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE);
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY,
        Oauth2MockServerBase.serverUri());
    conf.set(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY, "test");
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY, "xx:xx");
    conf.set(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY, "test");
  }

  @AfterAll
  public static void teardown() {
    Oauth2MockServerBase.tearDown();
  }
}
