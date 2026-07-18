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

package org.apache.gravitino.idp.auth;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.model.IdpUser;
import org.apache.gravitino.idp.web.rest.feature.IdpRESTFeature;
import org.junit.jupiter.api.Test;

class TestBasicAuthenticator {

  @Test
  void testValidateExtensionPackageOk() {
    Config config = new Config(false) {};
    config.set(
        Configs.REST_API_EXTENSION_PACKAGES,
        Lists.newArrayList(IdpRESTFeature.IDP_REST_EXTENSION_PACKAGE));

    assertDoesNotThrow(() -> BasicAuthenticator.validateExtensionPackage(config));
  }

  @Test
  void testValidateExtensionPackageMissingFails() {
    Config config = new Config(false) {};

    assertThrows(
        IllegalStateException.class, () -> BasicAuthenticator.validateExtensionPackage(config));
  }

  @Test
  void testNotInitialized() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class, () -> new BasicAuthenticator().authenticateToken(null));

    assertEquals("Basic authenticator has not been initialized", exception.getMessage());
  }

  @Test
  void testSupportsBasic() throws Exception {
    BasicAuthenticator authenticator = authenticator();

    assertTrue(authenticator.supportsToken(basicAuthBytes(basicAuthHeader("alice", "secret"))));
    assertFalse(authenticator.supportsToken("Bearer token".getBytes(StandardCharsets.UTF_8)));
    assertFalse(authenticator.supportsToken(null));
  }

  @Test
  void testEmptyHeader() throws Exception {
    BasicAuthenticator authenticator = authenticator();
    UnauthorizedException exception =
        assertThrows(UnauthorizedException.class, () -> authenticator.authenticateToken(null));

    assertEquals("Empty token authorization header", exception.getMessage());
    assertEquals("Basic", exception.getChallenges().get(0));
  }

  @Test
  void testMissingCredentials() throws Exception {
    BasicAuthenticator authenticator = authenticator();
    UnauthorizedException exception =
        assertThrows(
            UnauthorizedException.class,
            () ->
                authenticator.authenticateToken(
                    AuthConstants.AUTHORIZATION_BASIC_HEADER.getBytes(StandardCharsets.UTF_8)));

    assertEquals(
        "Malformed Basic authorization header: missing credentials", exception.getMessage());
    assertEquals("Basic", exception.getChallenges().get(0));
  }

  @Test
  void testValidCredentials() throws Exception {
    IdpUserGroupManager userGroupManager = mock(IdpUserGroupManager.class);
    when(userGroupManager.authenticate("alice", "Passw0rd-For-Alice"))
        .thenReturn(new IdpUser("alice", Arrays.asList("group-a", "group-b")));
    BasicAuthenticator authenticator = authenticator(userGroupManager);
    String authHeader = basicAuthHeader("alice", "Passw0rd-For-Alice");

    UserPrincipal principal =
        (UserPrincipal) authenticator.authenticateToken(basicAuthBytes(authHeader));

    assertEquals("alice", principal.getName());
    assertEquals(authHeader, principal.getAccessToken().orElse(null));
    assertEquals(2, principal.getGroups().size());
    assertEquals("group-a", principal.getGroups().get(0).getGroupName());
    assertEquals("group-b", principal.getGroups().get(1).getGroupName());
  }

  @Test
  void testUserNotFound() throws Exception {
    IdpUserGroupManager userGroupManager = mock(IdpUserGroupManager.class);
    when(userGroupManager.authenticate("alice", "Passw0rd-For-Alice"))
        .thenThrow(
            new UnauthorizedException(
                "Invalid username or password", AuthConstants.AUTHORIZATION_BASIC_HEADER.trim()));
    BasicAuthenticator authenticator = authenticator(userGroupManager);

    UnauthorizedException exception =
        assertThrows(
            UnauthorizedException.class,
            () ->
                authenticator.authenticateToken(
                    basicAuthBytes(basicAuthHeader("alice", "Passw0rd-For-Alice"))));

    assertUnauthorized(exception);
  }

  @Test
  void testInvalidBase64() throws Exception {
    BasicAuthenticator authenticator = authenticator();

    UnauthorizedException exception =
        assertThrows(
            UnauthorizedException.class,
            () -> authenticator.authenticateToken(basicAuthBytesWithCredential("not-valid!!!")));

    assertEquals("Malformed Basic authorization header: invalid base64", exception.getMessage());
    assertEquals("Basic", exception.getChallenges().get(0));
  }

  @Test
  void testMissingSeparator() throws Exception {
    BasicAuthenticator authenticator = authenticator();
    String credential =
        Base64.getEncoder().encodeToString("aliceonly".getBytes(StandardCharsets.UTF_8));

    UnauthorizedException exception =
        assertThrows(
            UnauthorizedException.class,
            () -> authenticator.authenticateToken(basicAuthBytesWithCredential(credential)));

    assertEquals(
        "Malformed Basic authorization header: credentials must be in username:password format",
        exception.getMessage());
    assertEquals("Basic", exception.getChallenges().get(0));
  }

  @Test
  void testEmptyUsername() throws Exception {
    BasicAuthenticator authenticator = authenticator();
    String credential =
        Base64.getEncoder().encodeToString(":password".getBytes(StandardCharsets.UTF_8));

    UnauthorizedException exception =
        assertThrows(
            UnauthorizedException.class,
            () -> authenticator.authenticateToken(basicAuthBytesWithCredential(credential)));

    assertEquals(
        "Malformed Basic authorization header: username must not be blank", exception.getMessage());
    assertEquals("Basic", exception.getChallenges().get(0));
  }

  @Test
  void testTrimmedCredential() throws Exception {
    IdpUserGroupManager userGroupManager = mock(IdpUserGroupManager.class);
    when(userGroupManager.authenticate("alice", "Passw0rd-For-Alice"))
        .thenReturn(new IdpUser("alice", Arrays.asList()));
    BasicAuthenticator authenticator = authenticator(userGroupManager);
    String credential =
        "  "
            + Base64.getEncoder()
                .encodeToString("alice:Passw0rd-For-Alice".getBytes(StandardCharsets.UTF_8))
            + "  ";

    UserPrincipal principal =
        (UserPrincipal) authenticator.authenticateToken(basicAuthBytesWithCredential(credential));

    assertEquals("alice", principal.getName());
  }

  @Test
  void testBlankPassword() throws Exception {
    IdpUserGroupManager userGroupManager = mock(IdpUserGroupManager.class);
    BasicAuthenticator authenticator = authenticator(userGroupManager);

    UnauthorizedException exception =
        assertThrows(
            UnauthorizedException.class,
            () -> authenticator.authenticateToken(basicAuthBytes(basicAuthHeader("alice", " "))));

    assertUnauthorized(exception);
  }

  @Test
  void testWrongPassword() throws Exception {
    IdpUserGroupManager userGroupManager = mock(IdpUserGroupManager.class);
    when(userGroupManager.authenticate("alice", "Passw0rd-For-Alice"))
        .thenThrow(
            new UnauthorizedException(
                "Invalid username or password", AuthConstants.AUTHORIZATION_BASIC_HEADER.trim()));
    BasicAuthenticator authenticator = authenticator(userGroupManager);

    UnauthorizedException exception =
        assertThrows(
            UnauthorizedException.class,
            () ->
                authenticator.authenticateToken(
                    basicAuthBytes(basicAuthHeader("alice", "Passw0rd-For-Alice"))));

    assertUnauthorized(exception);
  }

  private static void assertUnauthorized(UnauthorizedException exception) {
    assertEquals("Invalid username or password", exception.getMessage());
    assertEquals("Basic", exception.getChallenges().get(0));
  }

  private static String basicAuthHeader(String username, String password) {
    return AuthConstants.AUTHORIZATION_BASIC_HEADER
        + Base64.getEncoder()
            .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] basicAuthBytes(String authHeader) {
    return authHeader.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] basicAuthBytesWithCredential(String credential) {
    return (AuthConstants.AUTHORIZATION_BASIC_HEADER + credential).getBytes(StandardCharsets.UTF_8);
  }

  private BasicAuthenticator authenticator() throws Exception {
    return authenticator(mock(IdpUserGroupManager.class));
  }

  private static BasicAuthenticator authenticator(IdpUserGroupManager userGroupManager)
      throws Exception {
    BasicAuthenticator authenticator = new BasicAuthenticator();
    Field field = BasicAuthenticator.class.getDeclaredField("userGroupManager");
    field.setAccessible(true);
    field.set(authenticator, userGroupManager);
    return authenticator;
  }
}
