/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.auth;

import com.datastrato.gravitino.Config;
import java.util.Base64;
import org.apache.commons.lang3.StringUtils;

/**
 * SimpleAuthenticator will provide a basic authentication mechanism. SimpleAuthenticator will use
 * the identifier provided by the user without any validation.
 */
class SimpleAuthenticator implements Authenticator {

  @Override
  public boolean isDataFromToken() {
    return true;
  }

  @Override
  public String authenticateToken(byte[] tokenData) {
    if (tokenData == null) {
      return AuthConstants.UNKNOWN_USER_NAME;
    }
    String authData = new String(tokenData);
    if (StringUtils.isBlank(authData)) {
      return AuthConstants.UNKNOWN_USER_NAME;
    }
    if (!authData.startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER)) {
      return AuthConstants.UNKNOWN_USER_NAME;
    }
    String credential = authData.substring(AuthConstants.AUTHORIZATION_BASIC_HEADER.length());
    if (StringUtils.isBlank(credential)) {
      return AuthConstants.UNKNOWN_USER_NAME;
    }
    try {
      String[] userInformation = new String(Base64.getDecoder().decode(credential)).split(":");
      if (userInformation.length != 2) {
        return AuthConstants.UNKNOWN_USER_NAME;
      }
      return userInformation[0];
    } catch (IllegalArgumentException ie) {
      return AuthConstants.UNKNOWN_USER_NAME;
    }
  }

  @Override
  public void initialize(Config config) throws RuntimeException {
    // no op
  }
}
