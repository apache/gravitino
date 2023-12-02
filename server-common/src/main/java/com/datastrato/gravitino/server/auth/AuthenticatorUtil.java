/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.auth;

public class AuthenticatorUtil {

  private AuthenticatorUtil() {}

  public static boolean isEnableOAuth2() {
    Authenticator authenticator = ServerAuthenticator.getInstance().authenticator();
    return authenticator instanceof OAuth2TokenAuthenticator;
  }
}
