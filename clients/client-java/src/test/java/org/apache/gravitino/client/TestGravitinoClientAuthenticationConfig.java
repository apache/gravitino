/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.client;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestGravitinoClientAuthenticationConfig {

  private static final String SERVER_URI = "http://localhost:8090";

  @Test
  public void testNoAuthentication() {
    Map<String, String> config = ImmutableMap.of();
    AuthDataProvider result =
        GravitinoClientAuthenticationConfig.createAuthDataProvider(config, SERVER_URI);

    // When no auth type is specified, do not configure authentication
    Assertions.assertNull(result);
  }

  @Test
  public void testSimpleAuthenticationWithoutUser() {
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .put(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "simple")
            .build();

    AuthDataProvider result =
        GravitinoClientAuthenticationConfig.createAuthDataProvider(config, SERVER_URI);

    Assertions.assertNotNull(result);
    Assertions.assertTrue(result instanceof SimpleTokenProvider);
  }

  @Test
  public void testSimpleAuthenticationWithUser() {
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .put(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "simple")
            .put(GravitinoClientAuthenticationConfig.SIMPLE_AUTH_USER_KEY, "testuser")
            .build();

    AuthDataProvider result =
        GravitinoClientAuthenticationConfig.createAuthDataProvider(config, SERVER_URI);

    Assertions.assertNotNull(result);
    Assertions.assertTrue(result instanceof SimpleTokenProvider);
  }

  @Test
  public void testOAuthAuthenticationMissingScope() {
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .put(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "oauth")
            .put(
                GravitinoClientAuthenticationConfig.OAUTH_SERVER_URI_KEY,
                "http://oauth-server:8080")
            .put(
                GravitinoClientAuthenticationConfig.OAUTH_CREDENTIAL_KEY, "client_id:client_secret")
            .put(GravitinoClientAuthenticationConfig.OAUTH_PATH_KEY, "oauth2/token")
            .build();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> GravitinoClientAuthenticationConfig.createAuthDataProvider(config, SERVER_URI));

    Assertions.assertTrue(exception.getMessage().contains("OAuth scope is required"));
  }

  @Test
  public void testOAuthAuthenticationMissingServerUri() {
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .put(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "oauth")
            .put(
                GravitinoClientAuthenticationConfig.OAUTH_CREDENTIAL_KEY, "client_id:client_secret")
            .put(GravitinoClientAuthenticationConfig.OAUTH_PATH_KEY, "/oauth2/token")
            .build();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> GravitinoClientAuthenticationConfig.createAuthDataProvider(config, SERVER_URI));

    Assertions.assertTrue(exception.getMessage().contains("OAuth server URI is required"));
  }

  @Test
  public void testOAuthAuthenticationMissingCredential() {
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .put(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "oauth")
            .put(
                GravitinoClientAuthenticationConfig.OAUTH_SERVER_URI_KEY,
                "http://oauth-server:8080")
            .put(GravitinoClientAuthenticationConfig.OAUTH_PATH_KEY, "/oauth2/token")
            .build();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> GravitinoClientAuthenticationConfig.createAuthDataProvider(config, SERVER_URI));

    Assertions.assertTrue(exception.getMessage().contains("OAuth credential is required"));
  }

  @Test
  public void testOAuthAuthenticationMissingPath() {
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .put(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "oauth")
            .put(
                GravitinoClientAuthenticationConfig.OAUTH_SERVER_URI_KEY,
                "http://oauth-server:8080")
            .put(
                GravitinoClientAuthenticationConfig.OAUTH_CREDENTIAL_KEY, "client_id:client_secret")
            .build();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> GravitinoClientAuthenticationConfig.createAuthDataProvider(config, SERVER_URI));

    Assertions.assertTrue(exception.getMessage().contains("OAuth path is required"));
  }

  @Test
  public void testKerberosAuthenticationWithKeytab(@TempDir File tempDir) throws IOException {
    File keytabFile = new File(tempDir, "test.keytab");
    Files.createFile(keytabFile.toPath());

    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .put(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "kerberos")
            .put(GravitinoClientAuthenticationConfig.KERBEROS_PRINCIPAL_KEY, "user@REALM")
            .put(
                GravitinoClientAuthenticationConfig.KERBEROS_KEYTAB_FILE_PATH_KEY,
                keytabFile.getAbsolutePath())
            .build();

    AuthDataProvider result =
        GravitinoClientAuthenticationConfig.createAuthDataProvider(config, SERVER_URI);

    Assertions.assertNotNull(result);
    Assertions.assertTrue(result instanceof KerberosTokenProvider);
  }

  @Test
  public void testKerberosAuthenticationWithoutKeytab() {
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .put(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "kerberos")
            .put(GravitinoClientAuthenticationConfig.KERBEROS_PRINCIPAL_KEY, "user@REALM")
            .build();

    AuthDataProvider result =
        GravitinoClientAuthenticationConfig.createAuthDataProvider(config, SERVER_URI);

    Assertions.assertNotNull(result);
    Assertions.assertTrue(result instanceof KerberosTokenProvider);
  }

  @Test
  public void testKerberosAuthenticationMissingPrincipal() {
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .put(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "kerberos")
            .build();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> GravitinoClientAuthenticationConfig.createAuthDataProvider(config, SERVER_URI));

    Assertions.assertTrue(exception.getMessage().contains("Kerberos principal is required"));
  }

  @Test
  public void testKerberosAuthenticationKeytabFileNotFound() {
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .put(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "kerberos")
            .put(GravitinoClientAuthenticationConfig.KERBEROS_PRINCIPAL_KEY, "user@REALM")
            .put(
                GravitinoClientAuthenticationConfig.KERBEROS_KEYTAB_FILE_PATH_KEY,
                "/nonexistent/path/test.keytab")
            .build();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> GravitinoClientAuthenticationConfig.createAuthDataProvider(config, SERVER_URI));

    Assertions.assertTrue(exception.getMessage().contains("Keytab file not found"));
  }

  @Test
  public void testInvalidAuthenticationType() {
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .put(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "invalid_type")
            .build();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> GravitinoClientAuthenticationConfig.createAuthDataProvider(config, SERVER_URI));

    Assertions.assertTrue(exception.getMessage().contains("Invalid authentication type"));
  }

  @Test
  public void testNoneAuthenticationType() {
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .put(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "none")
            .build();

    AuthDataProvider result =
        GravitinoClientAuthenticationConfig.createAuthDataProvider(config, SERVER_URI);

    Assertions.assertNull(result);
  }
}
