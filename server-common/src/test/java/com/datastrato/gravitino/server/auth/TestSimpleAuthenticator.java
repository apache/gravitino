/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.auth;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.auth.AuthConstants;
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
        AuthConstants.ANONYMOUS_USER, simpleAuthenticator.authenticateToken(null).getName());
    Assertions.assertEquals(
        AuthConstants.ANONYMOUS_USER,
        simpleAuthenticator.authenticateToken("".getBytes(StandardCharsets.UTF_8)).getName());
    Assertions.assertEquals(
        AuthConstants.ANONYMOUS_USER,
        simpleAuthenticator.authenticateToken("abc".getBytes(StandardCharsets.UTF_8)).getName());
    Assertions.assertEquals(
        AuthConstants.ANONYMOUS_USER,
        simpleAuthenticator
            .authenticateToken(
                AuthConstants.AUTHORIZATION_BASIC_HEADER.getBytes(StandardCharsets.UTF_8))
            .getName());
    Assertions.assertEquals(
        AuthConstants.ANONYMOUS_USER,
        simpleAuthenticator
            .authenticateToken(
                (AuthConstants.AUTHORIZATION_BASIC_HEADER + "xx").getBytes(StandardCharsets.UTF_8))
            .getName());
    Assertions.assertEquals(
        "gravitino",
        simpleAuthenticator
            .authenticateToken(
                (AuthConstants.AUTHORIZATION_BASIC_HEADER
                        + new String(
                            Base64.getEncoder()
                                .encode("gravitino:gravitino".getBytes(StandardCharsets.UTF_8)),
                            StandardCharsets.UTF_8))
                    .getBytes(StandardCharsets.UTF_8))
            .getName());
  }
}
