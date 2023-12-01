/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client.auth;

import com.datastrato.gravitino.auth.AuthenticatorType;
import com.google.common.base.Preconditions;

public class AuthClientUtil {
  private AuthClientUtil() {}

  public static void checkAuthArgument(
      AuthenticatorType authenticatorType, AuthDataProvider authDataProvider) {
    if (AuthenticatorType.OAUTH == authenticatorType) {
      Preconditions.checkNotNull(authDataProvider, "OAuth mode must set AuthDataProvider");
      Preconditions.checkArgument(
          authDataProvider.hasTokenData(), "OAuthDataProvider must have token data");
      Preconditions.checkArgument(
          authDataProvider.getAuthType() == AuthenticatorType.OAUTH,
          "AuthDataProvider must be the type of OAuth");
    } else if (AuthenticatorType.SIMPLE == authenticatorType) {
      if (authDataProvider != null) {
        Preconditions.checkArgument(
            authDataProvider instanceof SimpleAuthDataProvider,
            "AuthDataProvider must be the instance of SimpleAuthDataProvider");
      }
    }
  }
}
