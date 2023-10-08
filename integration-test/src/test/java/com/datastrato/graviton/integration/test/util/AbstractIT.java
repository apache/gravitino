/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.test.util;

import static com.datastrato.graviton.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.graviton.server.GravitonServer.WEBSERVER_CONF_PREFIX;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.client.GravitonClient;
import com.datastrato.graviton.integration.test.MiniGraviton;
import com.datastrato.graviton.server.GravitonServer;
import com.datastrato.graviton.server.ServerConfig;
import com.datastrato.graviton.server.web.JettyServerContext;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(PrintFuncNameExtension.class)
public class AbstractIT {
  public static final Logger LOG = LoggerFactory.getLogger(AbstractIT.class);
  protected static GravitonClient client;

  private static MiniGraviton miniGraviton;

  private static final String TEST_MODE = "testMode";
  private static final String EMBEDDED_TEST_MODE = "embedded";

  protected static Config serverConfig;

  static String testMode =
      System.getProperty(TEST_MODE) == null ? EMBEDDED_TEST_MODE : System.getProperty(TEST_MODE);

  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    LOG.info("Running Graviton Server in {} mode", testMode);

    serverConfig = new ServerConfig();
    if (testMode != null && testMode.equals(EMBEDDED_TEST_MODE)) {
      miniGraviton = new MiniGraviton();
      miniGraviton.start();
      serverConfig = miniGraviton.getServerConfig();
    } else {
      serverConfig.loadFromFile(GravitonServer.CONF_FILE);

      try {
        FileUtils.deleteDirectory(
            FileUtils.getFile(serverConfig.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)));
      } catch (Exception e) {
        // Ignore
      }

      GravitonITUtils.startGravitonServer();
    }

    JettyServerContext serverContext =
        JettyServerContext.fromConfig(serverConfig, WEBSERVER_CONF_PREFIX);

    String uri = "http://" + serverContext.getHost() + ":" + serverContext.getHttpPort();
    client = GravitonClient.builder(uri).build();
  }

  @AfterAll
  public static void stopIntegrationTest() throws IOException, InterruptedException {
    if (client != null) {
      client.close();
    }
    if (testMode != null && testMode.equals(EMBEDDED_TEST_MODE)) {
      miniGraviton.stop();
    } else {
      GravitonITUtils.stopGravitonServer();
    }
    LOG.info("Tearing down Graviton Server");
  }
}
