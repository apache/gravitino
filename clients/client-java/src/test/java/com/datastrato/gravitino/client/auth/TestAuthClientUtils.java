/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client.auth;

import com.datastrato.gravitino.auth.AuthenticatorType;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAuthClientUtils {

  @Test
  public void testAuthClientUtils() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> AuthClientUtil.checkAuthArgument(AuthenticatorType.SIMPLE, new TestSimpleProvider()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> AuthClientUtil.checkAuthArgument(AuthenticatorType.SIMPLE, new TestOAuthProvider()));
    Assertions.assertDoesNotThrow(
        () ->
            AuthClientUtil.checkAuthArgument(
                AuthenticatorType.SIMPLE, new SimpleAuthDataProvider()));
    Assertions.assertDoesNotThrow(
        () -> AuthClientUtil.checkAuthArgument(AuthenticatorType.OAUTH, new TestOAuthProvider()));
  }

  private static class TestOAuthProvider implements AuthDataProvider {

    public TestOAuthProvider() {}

    @Override
    public void close() throws IOException {}

    @Override
    public boolean hasTokenData() {
      return true;
    }

    @Override
    public byte[] getTokenData() {
      return null;
    }

    @Override
    public AuthenticatorType getAuthType() {
      return AuthenticatorType.OAUTH;
    }
  }

  private static class TestSimpleProvider implements AuthDataProvider {

    @Override
    public boolean hasTokenData() {
      return AuthDataProvider.super.hasTokenData();
    }

    @Override
    public byte[] getTokenData() {
      return AuthDataProvider.super.getTokenData();
    }

    @Override
    public AuthenticatorType getAuthType() {
      return AuthenticatorType.SIMPLE;
    }

    @Override
    public void close() throws IOException {}
  }
}
