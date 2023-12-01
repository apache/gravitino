/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client.auth;

import com.datastrato.gravitino.auth.AuthenticatorType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/* *
 * SimpleAuthProvider will use the environment variable `GRAVITINO_USER` or
 * the user of the system to generate a basic token for every request.
 */
public final class SimpleAuthDataProvider implements AuthDataProvider {

  private final byte[] token;

  public SimpleAuthDataProvider() {
    String gravitinoUser = System.getenv("GRAVITINO_USER");
    if (gravitinoUser == null) {
      gravitinoUser = System.getProperty("user.name");
    }
    String userInformation = gravitinoUser + ":dummy";
    this.token = Base64.getEncoder().encode(userInformation.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public boolean hasTokenData() {
    return true;
  }

  @Override
  public byte[] getTokenData() {
    return token;
  }

  @Override
  public void close() throws IOException {
    // no op
  }

  @Override
  public AuthenticatorType getAuthType() {
    return AuthenticatorType.SIMPLE;
  }
}
