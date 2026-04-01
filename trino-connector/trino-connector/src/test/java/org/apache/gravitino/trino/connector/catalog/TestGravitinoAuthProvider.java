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
package org.apache.gravitino.trino.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@SuppressWarnings("deprecation")
public class TestGravitinoAuthProvider {

  @Test
  public void testBuildClientNoAuth() {
    assertDoesNotThrow(() -> GravitinoAuthProvider.buildClient(buildConfig(ImmutableMap.of())));
  }

  @Test
  public void testBuildClientNoneAuth() {
    assertDoesNotThrow(
        () ->
            GravitinoAuthProvider.buildClient(
                buildConfig(ImmutableMap.of(GravitinoAuthProvider.AUTH_TYPE_KEY, "none"))));
  }

  @Test
  public void testBuildClientSimpleAuthWithUser() {
    assertDoesNotThrow(
        () ->
            GravitinoAuthProvider.buildClient(
                buildConfig(
                    ImmutableMap.of(
                        GravitinoAuthProvider.AUTH_TYPE_KEY, "simple",
                        GravitinoAuthProvider.SIMPLE_AUTH_USER_KEY, "alice"))));
  }

  @Test
  public void testBuildClientSimpleAuthNoUser() {
    assertDoesNotThrow(
        () ->
            GravitinoAuthProvider.buildClient(
                buildConfig(ImmutableMap.of(GravitinoAuthProvider.AUTH_TYPE_KEY, "simple"))));
  }

  @Test
  public void testBuildClientInvalidAuthType() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoAuthProvider.buildClient(
                buildConfig(ImmutableMap.of(GravitinoAuthProvider.AUTH_TYPE_KEY, "invalid_type"))));
  }

  @Test
  public void testBuildClientOAuthMissingServerUri() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoAuthProvider.buildClient(
                buildConfig(
                    ImmutableMap.of(
                        GravitinoAuthProvider.AUTH_TYPE_KEY, "oauth2",
                        GravitinoAuthProvider.OAUTH_CREDENTIAL_KEY, "cred",
                        GravitinoAuthProvider.OAUTH_PATH_KEY, "oauth2/token",
                        GravitinoAuthProvider.OAUTH_SCOPE_KEY, "scope"))));
  }

  @Test
  public void testBuildClientOAuthMissingCredential() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoAuthProvider.buildClient(
                buildConfig(
                    ImmutableMap.of(
                        GravitinoAuthProvider.AUTH_TYPE_KEY, "oauth2",
                        GravitinoAuthProvider.OAUTH_SERVER_URI_KEY, "http://auth.example.com",
                        GravitinoAuthProvider.OAUTH_PATH_KEY, "oauth2/token",
                        GravitinoAuthProvider.OAUTH_SCOPE_KEY, "scope"))));
  }

  @Test
  public void testBuildClientKerberosMissingPrincipal() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoAuthProvider.buildClient(
                buildConfig(ImmutableMap.of(GravitinoAuthProvider.AUTH_TYPE_KEY, "kerberos"))));
  }

  @Test
  public void testBuildClientKerberosKeytabNotFound(@TempDir java.nio.file.Path tempDir) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoAuthProvider.buildClient(
                buildConfig(
                    ImmutableMap.of(
                        GravitinoAuthProvider.AUTH_TYPE_KEY, "kerberos",
                        GravitinoAuthProvider.KERBEROS_PRINCIPAL_KEY, "user@REALM.COM",
                        GravitinoAuthProvider.KERBEROS_KEYTAB_FILE_PATH_KEY,
                            tempDir.resolve("missing.keytab").toString()))));
  }

  @Test
  public void testBuildClientKerberosWithKeytab(@TempDir java.nio.file.Path tempDir)
      throws IOException {
    File keytabFile = tempDir.resolve("user.keytab").toFile();
    Files.write(keytabFile.toPath(), new byte[0]);
    assertDoesNotThrow(
        () ->
            GravitinoAuthProvider.buildClient(
                buildConfig(
                    ImmutableMap.of(
                        GravitinoAuthProvider.AUTH_TYPE_KEY, "kerberos",
                        GravitinoAuthProvider.KERBEROS_PRINCIPAL_KEY, "user@REALM.COM",
                        GravitinoAuthProvider.KERBEROS_KEYTAB_FILE_PATH_KEY,
                            keytabFile.getAbsolutePath()))));
  }

  @Test
  public void testBuildNoAuthReturnsNullSessionContext() {
    GravitinoAuthProvider.BuildResult result =
        GravitinoAuthProvider.build(buildConfig(ImmutableMap.of()));
    assertNotNull(result.client);
    assertNull(result.sessionContext);
  }

  @Test
  public void testBuildOAuth2TokenMissingCredentialKey() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoAuthProvider.build(
                buildConfig(ImmutableMap.of(GravitinoAuthProvider.AUTH_TYPE_KEY, "oauth2_token"))));
  }

  @Test
  public void testBuildOAuth2TokenReturnsSessionContext() {
    GravitinoAuthProvider.BuildResult result =
        GravitinoAuthProvider.build(
            buildConfig(
                ImmutableMap.of(
                    GravitinoAuthProvider.AUTH_TYPE_KEY, "oauth2_token",
                    GravitinoAuthProvider.OAUTH2_TOKEN_CREDENTIAL_KEY, "my-token-key")));
    assertNotNull(result.client);
    assertNotNull(result.sessionContext);
  }

  @Test
  public void testBuildSimpleWithForwardUserReturnsSessionContext() {
    GravitinoAuthProvider.BuildResult result =
        GravitinoAuthProvider.build(
            buildConfig(
                ImmutableMap.of(
                    GravitinoAuthProvider.AUTH_TYPE_KEY, "simple",
                    GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "true")));
    assertNotNull(result.client);
    assertNotNull(result.sessionContext);
  }

  @Test
  public void testBuildNoneWithForwardUserReturnsSessionContext() {
    GravitinoAuthProvider.BuildResult result =
        GravitinoAuthProvider.build(
            buildConfig(ImmutableMap.of(GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "true")));
    assertNotNull(result.client);
    assertNotNull(result.sessionContext);
  }

  private GravitinoConfig buildConfig(ImmutableMap<String, String> authConfig) {
    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
            .put("gravitino.uri", "http://127.0.0.1:8090")
            .put("gravitino.metalake", "test");
    builder.putAll(authConfig);
    return new GravitinoConfig(builder.build());
  }
}
