/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.datastrato.gravitino.auth.AuthConstants;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSimpleTokenProvider {

  @Test
  public void testAuthentication() throws IOException {
    try (AuthDataProvider provider = new SimpleTokenProvider()) {
      Assertions.assertTrue(provider.hasTokenData());
      String user = System.getenv("GRAVITINO_USER");
      String token = new String(provider.getTokenData(), StandardCharsets.UTF_8);
      Assertions.assertTrue(token.startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER));
      String tokenString =
          new String(
              Base64.getDecoder()
                  .decode(
                      token
                          .substring(AuthConstants.AUTHORIZATION_BASIC_HEADER.length())
                          .getBytes(StandardCharsets.UTF_8)),
              StandardCharsets.UTF_8);
      if (user != null) {
        Assertions.assertEquals(user + ":dummy", tokenString);
      } else {
        user = System.getProperty("user.name");
        Assertions.assertEquals(user + ":dummy", tokenString);
      }
    }
  }
}
