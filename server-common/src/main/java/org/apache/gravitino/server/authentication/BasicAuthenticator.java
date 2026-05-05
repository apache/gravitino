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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.Config;
import org.apache.gravitino.UserGroup;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.local.password.PasswordHasher;
import org.apache.gravitino.auth.local.password.PasswordHasherFactory;
import org.apache.gravitino.exceptions.BadRequestException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.service.IdpGroupMetaService;
import org.apache.gravitino.storage.relational.service.IdpUserMetaService;

public class BasicAuthenticator implements Authenticator {
  private static final String BASIC_AUTH_CHALLENGE = "Basic";

  private IdpUserMetaService userMetaService;
  private IdpGroupMetaService groupMetaService;
  private PasswordHasher passwordHasher;

  public BasicAuthenticator() {}

  @VisibleForTesting
  BasicAuthenticator(
      IdpUserMetaService userMetaService,
      IdpGroupMetaService groupMetaService,
      PasswordHasher passwordHasher) {
    this.userMetaService = userMetaService;
    this.groupMetaService = groupMetaService;
    this.passwordHasher = passwordHasher;
  }

  @Override
  public boolean isDataFromToken() {
    return true;
  }

  @Override
  public Principal authenticateToken(byte[] tokenData) {
    Preconditions.checkState(
        userMetaService != null && groupMetaService != null && passwordHasher != null,
        "Basic authenticator has not been initialized");
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

      String password = decodedCredential.substring(separatorIndex + 1);
      if (password.isEmpty()) {
        throw new UnauthorizedException("Invalid username or password", BASIC_AUTH_CHALLENGE);
      }
      IdpUserPO userPO =
          userMetaService
              .findUser(userName)
              .orElseThrow(
                  () ->
                      new UnauthorizedException(
                          "Invalid username or password", BASIC_AUTH_CHALLENGE));
      if (!passwordHasher.verify(password, userPO.getPasswordHash())) {
        throw new UnauthorizedException("Invalid username or password", BASIC_AUTH_CHALLENGE);
      }

      List<UserGroup> groups =
          groupMetaService.listGroupNames(userPO.getUserId()).stream()
              .map(groupName -> new UserGroup(Optional.empty(), groupName))
              .collect(Collectors.toList());
      return new UserPrincipal(userName, groups, authData);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e, "Malformed Basic authorization header: invalid base64");
    }
  }

  @Override
  public void initialize(Config config) {
    this.userMetaService = IdpUserMetaService.getInstance();
    this.groupMetaService = IdpGroupMetaService.getInstance();
    this.passwordHasher = PasswordHasherFactory.create();
  }

  @Override
  public boolean supportsToken(byte[] tokenData) {
    return tokenData != null
        && new String(tokenData, StandardCharsets.UTF_8)
            .startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER);
  }
}
