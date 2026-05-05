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

package org.apache.gravitino.server.authentication;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.local.password.PasswordHasher;
import org.apache.gravitino.exceptions.BadRequestException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.service.IdpUserMetaService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBasicAuthenticator {

  @Test
  public void testAuthenticateTokenThrowsWhenAuthenticatorNotInitialized() {
    IllegalStateException exception =
        Assertions.assertThrows(
            IllegalStateException.class, () -> new BasicAuthenticator().authenticateToken(null));

    Assertions.assertEquals("Basic authenticator has not been initialized", exception.getMessage());
  }

  @Test
  public void testAuthenticateTokenThrowsUnauthorizedWhenHeaderMissing() {
    BasicAuthenticator authenticator = authenticator();
    UnauthorizedException exception =
        Assertions.assertThrows(
            UnauthorizedException.class, () -> authenticator.authenticateToken(null));

    Assertions.assertEquals("Empty token authorization header", exception.getMessage());
    Assertions.assertEquals("Basic", exception.getChallenges().get(0));
  }

  @Test
  public void testAuthenticateTokenThrowsUnauthorizedWhenHeaderBlank() {
    BasicAuthenticator authenticator = authenticator();
    UnauthorizedException exception =
        Assertions.assertThrows(
            UnauthorizedException.class,
            () -> authenticator.authenticateToken(" ".getBytes(StandardCharsets.UTF_8)));

    Assertions.assertEquals("Empty token authorization header", exception.getMessage());
    Assertions.assertEquals("Basic", exception.getChallenges().get(0));
  }

  @Test
  public void testAuthenticateTokenThrowsBadRequestWhenBasicCredentialMissing() {
    BasicAuthenticator authenticator = authenticator();
    BadRequestException exception =
        Assertions.assertThrows(
            BadRequestException.class,
            () ->
                authenticator.authenticateToken(
                    AuthConstants.AUTHORIZATION_BASIC_HEADER.getBytes(StandardCharsets.UTF_8)));

    Assertions.assertEquals(
        "Malformed Basic authorization header: missing credentials", exception.getMessage());
  }

  @Test
  public void testAuthenticateTokenThrowsBadRequestWhenBasicCredentialIsMalformed() {
    BasicAuthenticator authenticator = authenticator();
    BadRequestException exception =
        Assertions.assertThrows(
            BadRequestException.class,
            () ->
                authenticator.authenticateToken(
                    (AuthConstants.AUTHORIZATION_BASIC_HEADER + "not-base64")
                        .getBytes(StandardCharsets.UTF_8)));

    Assertions.assertEquals(
        "Malformed Basic authorization header: invalid base64", exception.getMessage());
  }

  @Test
  public void testAuthenticateTokenReturnsPrincipalWhenCredentialsValid() {
    IdpUserMetaService userMetaService = mock(IdpUserMetaService.class);
    PasswordHasher passwordHasher = mock(PasswordHasher.class);
    IdpUserPO userPO = mock(IdpUserPO.class);
    when(userMetaService.findUser("alice")).thenReturn(java.util.Optional.of(userPO));
    when(userPO.getPasswordHash()).thenReturn("hash-1");
    when(passwordHasher.verify("Passw0rd-For-Alice", "hash-1")).thenReturn(true);
    BasicAuthenticator authenticator = new BasicAuthenticator(userMetaService, passwordHasher);
    String authHeader =
        AuthConstants.AUTHORIZATION_BASIC_HEADER
            + Base64.getEncoder()
                .encodeToString("alice:Passw0rd-For-Alice".getBytes(StandardCharsets.UTF_8));

    UserPrincipal principal =
        (UserPrincipal)
            authenticator.authenticateToken(authHeader.getBytes(StandardCharsets.UTF_8));

    Assertions.assertEquals("alice", principal.getName());
    Assertions.assertEquals(authHeader, principal.getAccessToken().orElse(null));
  }

  @Test
  public void testAuthenticateTokenThrowsUnauthorizedWhenUserMissing() {
    IdpUserMetaService userMetaService = mock(IdpUserMetaService.class);
    PasswordHasher passwordHasher = mock(PasswordHasher.class);
    when(userMetaService.findUser("alice")).thenReturn(java.util.Optional.empty());
    BasicAuthenticator authenticator = new BasicAuthenticator(userMetaService, passwordHasher);
    String authHeader =
        AuthConstants.AUTHORIZATION_BASIC_HEADER
            + Base64.getEncoder()
                .encodeToString("alice:Passw0rd-For-Alice".getBytes(StandardCharsets.UTF_8));

    UnauthorizedException exception =
        Assertions.assertThrows(
            UnauthorizedException.class,
            () -> authenticator.authenticateToken(authHeader.getBytes(StandardCharsets.UTF_8)));

    Assertions.assertEquals("Invalid username or password", exception.getMessage());
    Assertions.assertEquals("Basic", exception.getChallenges().get(0));
  }

  @Test
  public void testAuthenticateTokenThrowsUnauthorizedWhenPasswordMismatch() {
    IdpUserMetaService userMetaService = mock(IdpUserMetaService.class);
    PasswordHasher passwordHasher = mock(PasswordHasher.class);
    IdpUserPO userPO = mock(IdpUserPO.class);
    when(userMetaService.findUser("alice")).thenReturn(java.util.Optional.of(userPO));
    when(userPO.getPasswordHash()).thenReturn("hash-1");
    when(passwordHasher.verify("Passw0rd-For-Alice", "hash-1")).thenReturn(false);
    BasicAuthenticator authenticator = new BasicAuthenticator(userMetaService, passwordHasher);
    String authHeader =
        AuthConstants.AUTHORIZATION_BASIC_HEADER
            + Base64.getEncoder()
                .encodeToString("alice:Passw0rd-For-Alice".getBytes(StandardCharsets.UTF_8));

    UnauthorizedException exception =
        Assertions.assertThrows(
            UnauthorizedException.class,
            () -> authenticator.authenticateToken(authHeader.getBytes(StandardCharsets.UTF_8)));

    Assertions.assertEquals("Invalid username or password", exception.getMessage());
    Assertions.assertEquals("Basic", exception.getChallenges().get(0));
  }

  @Test
  public void testAuthenticateTokenThrowsUnauthorizedWhenPasswordEmpty() {
    IdpUserMetaService userMetaService = mock(IdpUserMetaService.class);
    PasswordHasher passwordHasher = mock(PasswordHasher.class);
    BasicAuthenticator authenticator = new BasicAuthenticator(userMetaService, passwordHasher);
    String authHeader =
        AuthConstants.AUTHORIZATION_BASIC_HEADER
            + Base64.getEncoder().encodeToString("alice:".getBytes(StandardCharsets.UTF_8));

    UnauthorizedException exception =
        Assertions.assertThrows(
            UnauthorizedException.class,
            () -> authenticator.authenticateToken(authHeader.getBytes(StandardCharsets.UTF_8)));

    Assertions.assertEquals("Invalid username or password", exception.getMessage());
    Assertions.assertEquals("Basic", exception.getChallenges().get(0));
  }

  private BasicAuthenticator authenticator() {
    return new BasicAuthenticator(mock(IdpUserMetaService.class), mock(PasswordHasher.class));
  }
}
