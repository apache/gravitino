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

package org.apache.gravitino.auth.local;

import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Base64;
import org.apache.gravitino.Config;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.exceptions.BadRequestException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.server.authentication.Authenticator;

/**
 * Basic authenticator wiring for the local authentication module.
 *
 * <p>The credential verification flow will be implemented in follow-up subtasks. For now, keep the
 * current Basic header parsing behavior so the module can be loaded safely when {@code
 * gravitino.authenticators=basic}.
 */
public class BasicAuthenticator implements Authenticator {
  private static final String BASIC_AUTH_CHALLENGE = "Basic";

  @Override
  public boolean isDataFromToken() {
    return true;
  }

  @Override
  public Principal authenticateToken(byte[] tokenData) {
    if (tokenData == null) {
      throw new UnauthorizedException("Empty token authorization header", BASIC_AUTH_CHALLENGE);
    }

    String authData = new String(tokenData, StandardCharsets.UTF_8);
    if (authData.trim().isEmpty()) {
      throw new UnauthorizedException("Empty token authorization header", BASIC_AUTH_CHALLENGE);
    }

    if (!authData.startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER)) {
      throw new UnauthorizedException("Invalid token authorization header", BASIC_AUTH_CHALLENGE);
    }

    String credential = authData.substring(AuthConstants.AUTHORIZATION_BASIC_HEADER.length());
    if (credential.trim().isEmpty()) {
      throw new BadRequestException("Malformed Basic authorization header: missing credentials");
    }

    try {
      String decodedCredential =
          new String(Base64.getDecoder().decode(credential), StandardCharsets.UTF_8);
      int separatorIndex = decodedCredential.indexOf(':');
      if (separatorIndex < 0) {
        throw new BadRequestException(
            "Malformed Basic authorization header: credentials must be in username:password format");
      }

      String userName = decodedCredential.substring(0, separatorIndex);
      if (userName.isEmpty()) {
        throw new BadRequestException(
            "Malformed Basic authorization header: username must not be empty");
      }

      return new UserPrincipal(userName, authData);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e, "Malformed Basic authorization header: invalid base64");
    }
  }

  @Override
  public void initialize(Config config) {
    // no-op
  }

  @Override
  public boolean supportsToken(byte[] tokenData) {
    return tokenData != null
        && new String(tokenData, StandardCharsets.UTF_8)
            .startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER);
  }
}
