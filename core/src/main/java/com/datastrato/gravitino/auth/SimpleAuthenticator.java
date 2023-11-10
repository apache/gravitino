/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.auth;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.utils.Constants;
import java.util.Base64;
import org.apache.commons.lang3.StringUtils;

class SimpleAuthenticator implements Authenticator {

  @Override
  public boolean isDataFromHTTP() {
    return true;
  }

  @Override
  public String authenticateHTTPHeader(String authData) {
    if (StringUtils.isBlank(authData)) {
      return Constants.UNKNOWN_USER_NAME;
    }
    if (!authData.startsWith(Constants.HTTP_HEADER_VALUE_BASIC_PREFIX)) {
      return Constants.UNKNOWN_USER_NAME;
    }
    String credential = authData.substring(Constants.HTTP_HEADER_VALUE_BASIC_PREFIX.length());
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
