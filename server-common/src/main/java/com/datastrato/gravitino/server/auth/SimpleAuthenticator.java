/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.auth;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.UserPrincipal;
import com.datastrato.gravitino.auth.AuthConstants;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Base64;
import org.apache.commons.lang3.StringUtils;

/**
 * SimpleAuthenticator will provide a basic authentication mechanism. SimpleAuthenticator will use
 * the identifier provided by the user without any validation.
 */
class SimpleAuthenticator implements Authenticator {

  private final Principal ANONYMOUS_PRINCIPAL = new UserPrincipal(AuthConstants.ANONYMOUS_USER);

  @Override
  public boolean isDataFromToken() {
    return true;
  }

  @Override
  public Principal authenticateToken(byte[] tokenData) {
    if (tokenData == null) {
      return ANONYMOUS_PRINCIPAL;
    }
    String authData = new String(tokenData, StandardCharsets.UTF_8);
    if (StringUtils.isBlank(authData)) {
      return ANONYMOUS_PRINCIPAL;
    }
    if (!authData.startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER)) {
      return ANONYMOUS_PRINCIPAL;
    }
    String credential = authData.substring(AuthConstants.AUTHORIZATION_BASIC_HEADER.length());
    if (StringUtils.isBlank(credential)) {
      return ANONYMOUS_PRINCIPAL;
    }
    try {
      String[] userInformation =
          new String(Base64.getDecoder().decode(credential), StandardCharsets.UTF_8).split(":");
      if (userInformation.length != 2) {
        return ANONYMOUS_PRINCIPAL;
      }
      return new UserPrincipal(userInformation[0]);
    } catch (IllegalArgumentException ie) {
      return ANONYMOUS_PRINCIPAL;
    }
  }

  @Override
  public void initialize(Config config) throws RuntimeException {
    // no op
  }
}
