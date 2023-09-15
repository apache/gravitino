/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.test;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.integration.test.util.ITUtils;
import com.datastrato.graviton.server.GravitonServer;
import com.datastrato.graviton.server.ServerConfig;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MiniGravitonServer extends GravitonServer {
  private static final Logger LOG = LoggerFactory.getLogger(MiniGravitonServer.class);

  private final File confDir;

  public MiniGravitonServer(String confFileDir) {
    this.confDir = new File(ITUtils.joinDirPath(confFileDir));
  }

  public static void main(String[] args) {
    LOG.info("Starting MiniGraviton Server");

    MiniGravitonServer server = new MiniGravitonServer(args[0]);
    server.overrideLoadConfFile();
    server.initialize();

    try {
      // Instantiates GravitonServer
      server.start();
    } catch (Exception e) {
      LOG.error("Error while running jettyServer", e);
      System.exit(-1);
    }
    LOG.info("Done, MiniGraviton server started.");

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    // Register some clean-up tasks that need to be done before shutting down
                    Thread.sleep(3000);
                  } catch (InterruptedException e) {
                    LOG.error("Interrupted exception:", e);
                  } catch (Exception e) {
                    LOG.error("Error while running clean-up tasks in shutdown hook", e);
                  }
                }));

    server.join();

    LOG.info("Shutting down MiniGraviton Server ... ");
    try {
      server.stop();
      LOG.info("MiniGraviton Server has shut down.");
    } catch (Exception e) {
      LOG.error("Error while stopping MiniGraviton Server", e);
    }
  }

  // User JAVA reflect method to load a special config file.
  void overrideLoadConfFile() {
    try {
      String confFile = ITUtils.joinDirPath(confDir.getAbsolutePath(), CONF_FILE);
      File fileConf = new File(confFile);
      if (!fileConf.exists()) {
        throw new IllegalArgumentException(
            "Config file " + fileConf.getAbsolutePath() + " not found");
      }

      ServerConfig serverConfig = new ServerConfig();
      Method loadPropertiesFromFileMethod =
          Config.class.getDeclaredMethod("loadPropertiesFromFile", File.class);
      loadPropertiesFromFileMethod.setAccessible(true);
      Object properties = loadPropertiesFromFileMethod.invoke(serverConfig, fileConf);
      LOG.info("Properties: {}", properties);

      Method loadFromPropertiesMethod =
          Config.class.getDeclaredMethod("loadFromProperties", Properties.class);
      loadFromPropertiesMethod.setAccessible(true);
      loadFromPropertiesMethod.invoke(serverConfig, properties);

      Field fieldServerConfig = GravitonServer.class.getDeclaredField("serverConfig");
      fieldServerConfig.setAccessible(true);
      fieldServerConfig.set(this, serverConfig);
    } catch (Exception e) {
      LOG.error("Exception in loading MiniGraviton Server config file ", e);
      throw new RuntimeException(e);
    }
  }
}
