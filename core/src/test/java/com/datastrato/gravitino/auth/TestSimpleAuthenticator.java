/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.auth;

import com.datastrato.gravitino.Config;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSimpleAuthenticator {

  @Test
  public void testAuthentication() {
    SimpleAuthenticator simpleAuthenticator = new SimpleAuthenticator();
    Config config = new Config(false) {};
    simpleAuthenticator.initialize(config);
    Assertions.assertTrue(simpleAuthenticator.isDataFromToken());
    Assertions.assertEquals(
        Constants.UNKNOWN_USER_NAME, simpleAuthenticator.authenticateToken(null));
    Assertions.assertEquals(
        Constants.UNKNOWN_USER_NAME,
        simpleAuthenticator.authenticateToken("".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertEquals(
        Constants.UNKNOWN_USER_NAME,
        simpleAuthenticator.authenticateToken("abc".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertEquals(
        Constants.UNKNOWN_USER_NAME,
        simpleAuthenticator.authenticateToken(
            Constants.HTTP_HEADER_AUTHORIZATION_BASIC.getBytes(StandardCharsets.UTF_8)));
    Assertions.assertEquals(
        Constants.UNKNOWN_USER_NAME,
        simpleAuthenticator.authenticateToken(
            (Constants.HTTP_HEADER_AUTHORIZATION_BASIC + "xx").getBytes(StandardCharsets.UTF_8)));
    Assertions.assertEquals(
        "gravitino",
        simpleAuthenticator.authenticateToken(
            (Constants.HTTP_HEADER_AUTHORIZATION_BASIC
                    + new String(
                        Base64.getEncoder()
                            .encode("gravitino:gravitino".getBytes(StandardCharsets.UTF_8))))
                .getBytes(StandardCharsets.UTF_8)));
  }
}
