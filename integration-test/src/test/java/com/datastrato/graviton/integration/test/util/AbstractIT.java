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
import com.datastrato.graviton.integration.test.MiniGravitonContext;
import com.datastrato.graviton.server.GravitonServer;
import com.datastrato.graviton.server.ServerConfig;
import com.datastrato.graviton.server.web.JettyServerConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
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
  protected static Config serverConfig;

  static String testMode = "";

  protected static Map<String, String> customConfigs = new HashMap<>();

  public static void registerCustomConfigs(Map<String, String> configs) {
    customConfigs.putAll(configs);
  }

  private static void rewriteGravitonServerConfig() throws IOException {
    if (customConfigs.isEmpty()) return;

    String gravitonHome = System.getenv("GRAVITON_HOME");

    String tmpFileName = GravitonServer.CONF_FILE + ".tmp";
    Path tmpPath = Paths.get(gravitonHome, "conf", tmpFileName);
    Files.deleteIfExists(tmpPath);

    Path configPath = Paths.get(gravitonHome, "conf", GravitonServer.CONF_FILE);
    Files.move(configPath, tmpPath);

    ITUtils.rewriteConfigFile(tmpPath.toString(), configPath.toString(), customConfigs);
  }

  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    testMode =
        System.getProperty(ITUtils.TEST_MODE) == null
            ? ITUtils.EMBEDDED_TEST_MODE
            : System.getProperty(ITUtils.TEST_MODE);

    LOG.info("Running Graviton Server in {} mode", testMode);

    serverConfig = new ServerConfig();
    if (testMode != null && testMode.equals(ITUtils.EMBEDDED_TEST_MODE)) {
      MiniGravitonContext context = new MiniGravitonContext(customConfigs);
      miniGraviton = new MiniGraviton(context);
      miniGraviton.start();
      serverConfig = miniGraviton.getServerConfig();
    } else {
      rewriteGravitonServerConfig();
      serverConfig.loadFromFile(GravitonServer.CONF_FILE);

      try {
        FileUtils.deleteDirectory(
            FileUtils.getFile(serverConfig.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)));
      } catch (Exception e) {
        // Ignore
      }

      GravitonITUtils.startGravitonServer();
    }

    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, WEBSERVER_CONF_PREFIX);

    String uri = "http://" + jettyServerConfig.getHost() + ":" + jettyServerConfig.getHttpPort();
    client = GravitonClient.builder(uri).build();
  }

  @AfterAll
  public static void stopIntegrationTest() throws IOException, InterruptedException {
    if (client != null) {
      client.close();
    }
    if (testMode != null && testMode.equals(ITUtils.EMBEDDED_TEST_MODE)) {
      miniGraviton.stop();
    } else {
      GravitonITUtils.stopGravitonServer();
    }
    LOG.info("Tearing down Graviton Server");
  }
}
