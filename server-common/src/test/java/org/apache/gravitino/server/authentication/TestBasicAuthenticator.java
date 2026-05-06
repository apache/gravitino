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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.exceptions.BadRequestException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBasicAuthenticator {

  private final BasicAuthenticator authenticator = new BasicAuthenticator();

  @Test
  public void testAuthenticateTokenThrowsBadRequestWhenBasicCredentialMissing() {
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
  public void testAuthenticateTokenThrowsBadRequestWhenBasicCredentialContainsExtraSpaces() {
    String credentials =
        Base64.getEncoder().encodeToString("user:password".getBytes(StandardCharsets.UTF_8));

    BadRequestException exception =
        Assertions.assertThrows(
            BadRequestException.class,
            () ->
                authenticator.authenticateToken(
                    (AuthConstants.AUTHORIZATION_BASIC_HEADER + "  " + credentials)
                        .getBytes(StandardCharsets.UTF_8)));

    Assertions.assertEquals(
        "Malformed Basic authorization header: invalid base64", exception.getMessage());
  }

  @Test
  public void testAuthenticateTokenThrowsBadRequestWhenUserNameMissing() {
    String credentials =
        Base64.getEncoder().encodeToString(":password".getBytes(StandardCharsets.UTF_8));

    BadRequestException exception =
        Assertions.assertThrows(
            BadRequestException.class,
            () ->
                authenticator.authenticateToken(
                    (AuthConstants.AUTHORIZATION_BASIC_HEADER + credentials)
                        .getBytes(StandardCharsets.UTF_8)));

    Assertions.assertEquals(
        "Malformed Basic authorization header: username must not be empty", exception.getMessage());
  }

  @Test
  public void testAuthenticateTokenReturnsPrincipalWhenCredentialValid() {
    String credentials =
        Base64.getEncoder().encodeToString("user:password".getBytes(StandardCharsets.UTF_8));

    UserPrincipal principal =
        (UserPrincipal)
            authenticator.authenticateToken(
                (AuthConstants.AUTHORIZATION_BASIC_HEADER + credentials)
                    .getBytes(StandardCharsets.UTF_8));

    Assertions.assertEquals("user", principal.getName());
    Assertions.assertEquals(
        AuthConstants.AUTHORIZATION_BASIC_HEADER + credentials,
        principal.getAccessToken().orElseThrow());
  }
}
