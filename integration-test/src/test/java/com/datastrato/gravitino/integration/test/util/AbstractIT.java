/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.util;

import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.gravitino.server.GravitinoServer.WEBSERVER_CONF_PREFIX;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.integration.test.MiniGravitino;
import com.datastrato.gravitino.server.GravitinoServer;
import com.datastrato.gravitino.server.ServerConfig;
import com.datastrato.gravitino.server.web.JettyServerConfig;
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
  protected static GravitinoClient client;

  private static MiniGravitino miniGravitino;

  private static final String TEST_MODE = "testMode";
  private static final String EMBEDDED_TEST_MODE = "embedded";

  protected static Config serverConfig;

  static String testMode = "";

  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    testMode =
        System.getProperty(TEST_MODE) == null ? EMBEDDED_TEST_MODE : System.getProperty(TEST_MODE);

    LOG.info("Running Gravitino Server in {} mode", testMode);

    serverConfig = new ServerConfig();
    if (testMode != null && testMode.equals(EMBEDDED_TEST_MODE)) {
      miniGravitino = new MiniGravitino();
      miniGravitino.start();
      serverConfig = miniGravitino.getServerConfig();
    } else {
      serverConfig.loadFromFile(GravitinoServer.CONF_FILE);

      try {
        FileUtils.deleteDirectory(
            FileUtils.getFile(serverConfig.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)));
      } catch (Exception e) {
        // Ignore
      }

      GravitinoITUtils.startGravitinoServer();
    }

    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, WEBSERVER_CONF_PREFIX);

    String uri = "http://" + jettyServerConfig.getHost() + ":" + jettyServerConfig.getHttpPort();
    client = GravitinoClient.builder(uri).build();
  }

  @AfterAll
  public static void stopIntegrationTest() throws IOException, InterruptedException {
    if (client != null) {
      client.close();
    }
    if (testMode != null && testMode.equals(EMBEDDED_TEST_MODE)) {
      miniGravitino.stop();
    } else {
      GravitinoITUtils.stopGravitinoServer();
    }
    LOG.info("Tearing down Gravitino Server");
  }
}
