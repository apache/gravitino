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
import org.apache.gravitino.UserGroup;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.exceptions.BadRequestException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.basic.password.PasswordHasherFactory;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.idp.storage.service.IdpUserMetaService;
import org.apache.gravitino.server.authentication.Authenticator;

/** Authenticates HTTP Basic credentials against built-in IdP user metadata. */
public class BasicAuthenticator implements Authenticator {

  private static final String BASIC_CHALLENGE = AuthConstants.AUTHORIZATION_BASIC_HEADER.trim();

  private IdpUserMetaService userMetaService;
  private PasswordHasher passwordHasher;

  /** Creates a {@link BasicAuthenticator} for reflective loading. */
  public BasicAuthenticator() {}

  BasicAuthenticator(IdpUserMetaService userMetaService, PasswordHasher passwordHasher) {
    this.userMetaService = userMetaService;
    this.passwordHasher = passwordHasher;
  }

  @Override
  public boolean isDataFromToken() {
    return true;
  }

  @Override
  public Principal authenticateToken(byte[] tokenData) {
    Preconditions.checkState(
        userMetaService != null && passwordHasher != null,
        "Basic authenticator has not been initialized");
    String authData = requireBasicAuthHeader(tokenData);
    BasicCredentials credentials = parseBasicCredentials(authData);
    return authenticate(credentials, authData);
  }

  @Override
  public void initialize(Config config) {
    this.userMetaService = IdpUserMetaService.getInstance();
    this.passwordHasher = PasswordHasherFactory.create();
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

      String password = decodedCredential.substring(separatorIndex + 1);
      if (StringUtils.isBlank(password)) {
        throw invalidCredentials();
      }
      return new BasicCredentials(userName, password);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e, "Malformed Basic authorization header: invalid base64");
    }
  }

  private UserPrincipal authenticate(BasicCredentials credentials, String authData) {
    IdpUserPO userPO = loadUser(credentials.userName());
    if (!passwordHasher.verify(credentials.password(), userPO.getPasswordHash())) {
      throw invalidCredentials();
    }

    List<UserGroup> groups =
        userMetaService.listGroupNamesByUsername(credentials.userName()).stream()
            .map(groupName -> new UserGroup(Optional.empty(), groupName))
            .collect(Collectors.toList());
    return new UserPrincipal(credentials.userName(), groups, authData);
  }

  private IdpUserPO loadUser(String userName) {
    try {
      return userMetaService.getIdpUserByUsername(userName);
    } catch (NotFoundException e) {
      throw invalidCredentials();
    }
  }

  private static UnauthorizedException unauthorized(String message) {
    return new UnauthorizedException(message, BASIC_CHALLENGE);
  }

  private static UnauthorizedException invalidCredentials() {
    return unauthorized("Invalid username or password");
  }

  private static final class BasicCredentials {
    private final String userName;
    private final String password;

    private BasicCredentials(String userName, String password) {
      this.userName = userName;
      this.password = password;
    }

    private String userName() {
      return userName;
    }

    private String password() {
      return password;
    }
  }
}
