/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestGravitonServer {

  private GravitonServer gravitonServer;

  @BeforeEach
  public void setUp() {
    gravitonServer = new GravitonServer();
  }

  @AfterEach
  public void tearDown() {
    if (gravitonServer != null) {
      gravitonServer.stop();
    }
  }

  @Test
  public void testInitialize() {
    gravitonServer.initialize();
  }

  @Test
  public void testStartAndStop() throws Exception {
    gravitonServer.initialize();
    gravitonServer.start();
    gravitonServer.stop();
  }

  @Test
  public void testStartWithoutInitialise() throws Exception {
    assertThrows(GravitonServerException.class, () -> gravitonServer.start());
  }

  @Test
  public void testStopBeforStart() throws Exception {
    gravitonServer.stop();
  }

  @Test
  public void testInitializeWithLoadFromFileException() throws Exception {
    ServerConfig config = new ServerConfig();

    // TODO: Exception due to environment variable not set. Is this the right exception?
    assertThrows(IllegalArgumentException.class, () -> config.loadFromFile("config"));
  }
}
