/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client.auth;

import com.datastrato.gravitino.auth.AuthenticatorType;
import com.google.common.base.Preconditions;

// Referred from Apache Iceberg's OAuth2Util implementation
// core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Util.java
public class AuthClientUtil {
  private AuthClientUtil() {}

  public static void checkAuthArgument(
      AuthenticatorType authenticatorType, AuthDataProvider authDataProvider) {
    if (AuthenticatorType.OAUTH.equals(authenticatorType)) {
      Preconditions.checkNotNull(authDataProvider, "OAuth mode must set AuthDataProvider");
      Preconditions.checkArgument(
          authDataProvider.hasTokenData(), "OAuthDataProvider must have token data");
    } else if (AuthenticatorType.SIMPLE.equals(authenticatorType)) {
      Preconditions.checkArgument(
          authDataProvider == null, "Simple mode shouldn't set AuthDataProvider");
    }
  }
}
