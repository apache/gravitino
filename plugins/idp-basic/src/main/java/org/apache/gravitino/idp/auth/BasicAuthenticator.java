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

import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.UserGroup;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.exceptions.BadRequestException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.model.IdpUser;
import org.apache.gravitino.server.authentication.Authenticator;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;

/** Authenticates HTTP Basic credentials against built-in IdP user metadata. */
public class BasicAuthenticator implements Authenticator {

  private static final String BASIC_CHALLENGE = AuthConstants.AUTHORIZATION_BASIC_HEADER.trim();

  private IdpUserGroupManager userGroupManager;

  /** Creates a {@link BasicAuthenticator} for reflective loading by {@link Authenticator}. */
  public BasicAuthenticator() {}

  @Override
  public boolean isDataFromToken() {
    return true;
  }

  @Override
  public Principal authenticateToken(byte[] tokenData) {
    Preconditions.checkState(
        userGroupManager != null, "Basic authenticator has not been initialized");
    String authData = requireBasicAuthHeader(tokenData);
    BasicCredentials credentials = parseBasicCredentials(authData);
    return authenticate(credentials, authData);
  }

  @Override
  public void initialize(Config config) {
    GravitinoEnv env = GravitinoEnv.getInstance();
    IdGenerator idGenerator =
        env.idGenerator() != null ? env.idGenerator() : RandomIdGenerator.INSTANCE;
    this.userGroupManager = new IdpUserGroupManager(config, idGenerator);
  }

  @Override
  public boolean supportsToken(byte[] tokenData) {
    return tokenData != null
        && new String(tokenData, StandardCharsets.UTF_8)
            .startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER);
  }

  private String requireBasicAuthHeader(byte[] tokenData) {
    if (tokenData == null) {
      throw unauthorized("Empty token authorization header");
    }

    String authData = new String(tokenData, StandardCharsets.UTF_8);
    if (authData.trim().isEmpty()) {
      throw unauthorized("Empty token authorization header");
    }
    if (!authData.startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER)) {
      throw unauthorized("Invalid token authorization header");
    }
    return authData;
  }

  private BasicCredentials parseBasicCredentials(String authData) {
    String credential = authData.substring(AuthConstants.AUTHORIZATION_BASIC_HEADER.length());
    credential = credential.trim();
    if (credential.isEmpty()) {
      throw new BadRequestException("Malformed Basic authorization header: missing credentials");
    }

    try {
      String decodedCredential =
          new String(Base64.getDecoder().decode(credential), StandardCharsets.UTF_8);
      String[] parts = decodedCredential.split(":", 2);
      if (parts.length != 2) {
        throw new BadRequestException(
            "Malformed Basic authorization header: credentials must be in username:password format");
      }

      String username = parts[0];
      if (username.isEmpty()) {
        throw new BadRequestException(
            "Malformed Basic authorization header: username must not be empty");
      }

      String password = parts[1];
      if (StringUtils.isBlank(password)) {
        throw invalidCredentials();
      }
      return new BasicCredentials(username, password);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e, "Malformed Basic authorization header: invalid base64");
    }
  }

  private UserPrincipal authenticate(BasicCredentials credentials, String authData) {
    IdpUser user = userGroupManager.authenticate(credentials.username(), credentials.password());
    if (user == null) {
      throw invalidCredentials();
    }

    List<UserGroup> groups =
        user.groupNames().stream()
            .map(groupName -> new UserGroup(Optional.empty(), groupName))
            .collect(Collectors.toList());
    return new UserPrincipal(user.name(), groups, authData);
  }

  private static UnauthorizedException unauthorized(String message) {
    return new UnauthorizedException(message, BASIC_CHALLENGE);
  }

  private static UnauthorizedException invalidCredentials() {
    return unauthorized("Invalid username or password");
  }

  private static final class BasicCredentials {
    private final String username;
    private final String password;

    private BasicCredentials(String username, String password) {
      this.username = username;
      this.password = password;
    }

    private String username() {
      return username;
    }

    private String password() {
      return password;
    }
  }
}
