/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.util;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.client.GravitonClient;
import com.datastrato.graviton.server.GravitonServer;
import com.datastrato.graviton.server.ServerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractIT {
  public static final Logger LOG = LoggerFactory.getLogger(AbstractIT.class);
  protected static GravitonClient client;

  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    LOG.info("Starting up Graviton Server");
    GravitonITUtils.startGravitonServer();

    Config serverConfig = new ServerConfig();
    serverConfig.loadFromFile(GravitonServer.CONF_FILE);

    String uri =
        "http://"
            + serverConfig.get(ServerConfig.WEBSERVER_HOST)
            + ":"
            + serverConfig.get(ServerConfig.WEBSERVER_HTTP_PORT);
    client = GravitonClient.builder(uri).build();
  }

  @AfterAll
  public static void stopIntegrationTest() {
    client.close();
    GravitonITUtils.stopGravitonServer();
    LOG.info("Tearing down Graviton Server");
  }
}
