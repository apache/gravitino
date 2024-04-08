/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mockserver.integration.ClientAndServer;

public abstract class Oauth2MockServerBase {
  private static ClientAndServer mockServer;
  private static final String MOCK_SERVER_HOST = "http://127.0.0.1:";
  private static int port;

  @BeforeAll
  public static void setup() {
    mockServer = ClientAndServer.startClientAndServer(0);
    port = mockServer.getLocalPort();
  }

  @AfterAll
  public static void tearDown() {
    mockServer.stop();
  }

  public static String serverUri() {
    return String.format("%s%d", MOCK_SERVER_HOST, port);
  }

  public static ClientAndServer mockServer() {
    return mockServer;
  }
}
