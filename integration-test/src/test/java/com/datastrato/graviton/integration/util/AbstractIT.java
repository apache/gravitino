/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.util;

import com.datastrato.graviton.client.GravitonClient;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractIT {
  public static final Logger LOG = LoggerFactory.getLogger(AbstractIT.class);
  private static final int port = 8090;
  protected static GravitonClient client;

  @BeforeAll
  public static void startIntegrationTest() {
    LOG.info("Starting up Graviton Server");
    GravitonITUtils.startGravitonServer();

    client = GravitonClient.builder("http://127.0.0.1:" + port).build();
  }

  @AfterAll
  public static void stopIntegrationTest() throws IOException {
    client.close();
    GravitonITUtils.stopGravitonServer();
    LOG.info("Tearing down Graviton Server");
  }
}
