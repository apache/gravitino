/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client.auth;

import java.io.IOException;
import java.util.Base64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSimpleAuthDataProvider {

  @Test
  public void testAuthentication() throws IOException {
    try (AuthDataProvider provider = new SimpleAuthDataProvider()) {
      Assertions.assertTrue(provider.hasTokenData());
      String user = System.getenv("GRAVITINO_USER");
      String tokenString = new String(Base64.getDecoder().decode(provider.getTokenData()));
      if (user != null) {
        Assertions.assertEquals(user + ":dummy", tokenString);
      } else {
        user = System.getProperty("user.name");
        Assertions.assertEquals(user + ":dummy", tokenString);
      }
    }
  }
}
