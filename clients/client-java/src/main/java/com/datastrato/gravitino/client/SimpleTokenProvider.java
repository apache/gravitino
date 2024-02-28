/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.auth.AuthConstants;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/* *
 * SimpleAuthProvider will use the environment variable `GRAVITINO_USER` or
 * the user of the system to generate a basic token for every request.
 */
final class SimpleTokenProvider implements AuthDataProvider {

  private final byte[] token;

  public SimpleTokenProvider() {
    String gravitinoUser = System.getenv("GRAVITINO_USER");
    if (gravitinoUser == null) {
      gravitinoUser = System.getProperty("user.name");
    }
    String userInformation = gravitinoUser + ":dummy";
    this.token =
        (AuthConstants.AUTHORIZATION_BASIC_HEADER
                + new String(
                    Base64.getEncoder().encode(userInformation.getBytes(StandardCharsets.UTF_8)),
                    StandardCharsets.UTF_8))
            .getBytes(StandardCharsets.UTF_8);
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
}
