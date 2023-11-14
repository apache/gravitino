/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.auth;

import com.datastrato.gravitino.Config;
import java.util.Base64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSimpleAuthenticator {

  @Test
  public void testAuthentication() {
    SimpleAuthenticator simpleAuthenticator = new SimpleAuthenticator();
    Config config = new Config(false) {};
    simpleAuthenticator.initialize(config);
    Assertions.assertTrue(simpleAuthenticator.isDataFromHTTP());
    Assertions.assertEquals(
        Constants.UNKNOWN_USER_NAME, simpleAuthenticator.authenticateHTTPHeader(""));
    Assertions.assertEquals(
        Constants.UNKNOWN_USER_NAME, simpleAuthenticator.authenticateHTTPHeader("abc"));
    Assertions.assertEquals(
        Constants.UNKNOWN_USER_NAME,
        simpleAuthenticator.authenticateHTTPHeader(Constants.HTTP_HEADER_AUTHORIZATION_BASIC));
    Assertions.assertEquals(
        Constants.UNKNOWN_USER_NAME,
        simpleAuthenticator.authenticateHTTPHeader(
            Constants.HTTP_HEADER_AUTHORIZATION_BASIC + "xx"));
    Assertions.assertEquals(
        "gravitino",
        simpleAuthenticator.authenticateHTTPHeader(
            Constants.HTTP_HEADER_AUTHORIZATION_BASIC
                + new String(Base64.getEncoder().encode("gravitino:gravitino".getBytes()))));
  }
}
