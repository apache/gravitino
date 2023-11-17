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
    String authData = new String(tokenData);
    if (StringUtils.isBlank(authData)) {
      return Constants.UNKNOWN_USER_NAME;
    }
    if (!authData.startsWith(Constants.HTTP_HEADER_AUTHORIZATION_BASIC)) {
      return Constants.UNKNOWN_USER_NAME;
    }
    String credential = authData.substring(Constants.HTTP_HEADER_AUTHORIZATION_BASIC.length());
    if (StringUtils.isBlank(credential)) {
      return Constants.UNKNOWN_USER_NAME;
    }
    try {
      String[] userInformation = new String(Base64.getDecoder().decode(credential)).split(":");
      if (userInformation.length != 2) {
        return Constants.UNKNOWN_USER_NAME;
      }
      return userInformation[0];
    } catch (IllegalArgumentException ie) {
      return Constants.UNKNOWN_USER_NAME;
    }
  }

  @Override
  public void initialize(Config config) throws RuntimeException {
    // no op
  }
}
