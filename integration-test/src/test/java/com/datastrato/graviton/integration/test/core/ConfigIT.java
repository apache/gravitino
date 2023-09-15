/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.test.core;

import static com.datastrato.graviton.server.GravitonServer.CONF_FILE;
import static com.datastrato.graviton.server.ServerConfig.WEBSERVER_HOST;
import static com.datastrato.graviton.server.ServerConfig.WEBSERVER_HTTP_PORT;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.graviton.integration.test.util.AbstractIT;
import com.datastrato.graviton.server.ServerConfig;
import java.util.Objects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConfigIT extends AbstractIT {
  @Test
  public void testLoadFromFile() throws Exception {
    String testMode = System.getProperty("TestMode");

    if (testMode == null || testMode.equals("embedded")) {
      // MiniGravitonServer user reflect method to load a special config file.
      // So `ServerConfig::loadFromFile()` must be throws `IllegalArgumentException`.
      ServerConfig serverConfig = new ServerConfig();
      assertThrows(IllegalArgumentException.class, () -> serverConfig.loadFromFile(CONF_FILE));
    } else if (Objects.equals(testMode, "deploy")) {
      ServerConfig serverConfig = new ServerConfig();
      serverConfig.loadFromFile(CONF_FILE);
      Assertions.assertEquals(WEBSERVER_HOST.getDefaultValue(), serverConfig.get(WEBSERVER_HOST));
      Assertions.assertEquals(
          WEBSERVER_HTTP_PORT.getDefaultValue(), serverConfig.get(WEBSERVER_HTTP_PORT));
    }
  }
}
