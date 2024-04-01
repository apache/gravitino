/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.datastrato.gravitino.client.DefaultOAuth2TokenProvider;
import com.datastrato.gravitino.client.ErrorHandlers;
import com.datastrato.gravitino.client.HTTPClient;
import com.datastrato.gravitino.dto.responses.OAuth2TokenResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestOauth2Client extends TestGvfsBase {
  private static final String path = "test";
  private static final String credential = "xx:xx";
  private static final String scope = "test";

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
    conf.set(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY, path);
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY,
        credential);
    conf.set(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY, scope);
  }

  @AfterAll
  public static void teardown() {
    Oauth2MockServerBase.tearDown();
  }

  @Test
  public void testOauth2ServerResponse() {
    DefaultOAuth2TokenProvider authDataProvider =
        DefaultOAuth2TokenProvider.builder()
            .withUri(Oauth2MockServerBase.serverUri())
            .withCredential(credential)
            .withPath(path)
            .withScope(scope)
            .build();

    HTTPClient client =
        HTTPClient.builder(new HashMap<>())
            .uri(Oauth2MockServerBase.serverUri())
            .withAuthDataProvider(authDataProvider)
            .build();

    OAuth2TokenResponse response =
        client.get(
            "test",
            OAuth2TokenResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.restErrorHandler());
    assertNotNull(response);
    assertEquals(Oauth2MockServerBase.accessToken(), response.getAccessToken());
    assertEquals("2", response.getIssuedTokenType());
    assertEquals("bearer", response.getTokenType());
    assertEquals("test", response.getScope());
    assertEquals(1, response.getExpiresIn());
  }

  @Test
  public void testConfiguration() throws IOException {
    try (FileSystem fs = managedFilesetPath.getFileSystem(conf)) {
      assertNotNull(
          fs.getConf()
              .get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY));
      assertEquals(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          fs.getConf()
              .get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY));

      assertNotNull(
          fs.getConf()
              .get(
                  GravitinoVirtualFileSystemConfiguration
                      .FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY));
      assertEquals(
          Oauth2MockServerBase.serverUri(),
          fs.getConf()
              .get(
                  GravitinoVirtualFileSystemConfiguration
                      .FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY));

      assertNotNull(
          fs.getConf()
              .get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY));
      assertEquals(
          path,
          fs.getConf()
              .get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY));

      assertNotNull(
          fs.getConf()
              .get(
                  GravitinoVirtualFileSystemConfiguration
                      .FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY));
      assertEquals(
          credential,
          fs.getConf()
              .get(
                  GravitinoVirtualFileSystemConfiguration
                      .FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY));

      assertNotNull(
          fs.getConf()
              .get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY));
      assertEquals(
          scope,
          fs.getConf()
              .get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY));
    }
  }
}
