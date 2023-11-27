/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client.auth;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/* *
 * SimpleAuthProvider will use the environment variable `GRAVITINO_USER` or
 * the user of the system to generate a basic token for every request.
 */
public class SimpleAuthDataProvider implements AuthDataProvider {

  private final byte[] token;
  private boolean isInitialized = false;

  @Override
  public void initialize(Map<String, String> properties) {
    isInitialized = true;
  }

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
    if (!isInitialized) {
      throw new IllegalStateException("SimpleDataProvider isn't initialized yet");
    }
    return true;
  }

  @Override
  public byte[] getTokenData() {
    if (!isInitialized) {
      throw new IllegalStateException("SimpleDataProvider isn't initialized yet");
    }
    return token;
  }

  @Override
  public void close() throws IOException {
    // no op
  }
}
