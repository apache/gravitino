/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.rest.RESTUtils;
import java.io.IOException;
import org.glassfish.jersey.test.JerseyTest;

public class JerseyTestBase extends JerseyTest {
  static {
    try {
      int port = RESTUtils.findAvailablePort(2000, 3000);
      System.setProperty("jersey.config.test.container.port", String.valueOf(port));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
