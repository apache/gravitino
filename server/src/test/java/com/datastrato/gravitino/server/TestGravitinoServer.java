/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server;

import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.gravitino.auxiliary.AuxiliaryServiceManager;
import com.datastrato.gravitino.rest.RESTUtils;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.mockito.Mockito;

@TestInstance(Lifecycle.PER_CLASS)
public class TestGravitinoServer {

  private GravitinoServer gravitinoServer;
  private final String ROCKS_DB_STORE_PATH =
      "/tmp/gravitino_test_server_" + UUID.randomUUID().toString().replace("-", "");
  private ServerConfig spyServerConfig;

  @BeforeAll
  void initConfig() throws IOException {
    ServerConfig serverConfig = new ServerConfig();
    serverConfig.loadFromMap(
        ImmutableMap.of(
            ENTRY_KV_ROCKSDB_BACKEND_PATH.getKey(),
            ROCKS_DB_STORE_PATH,
            GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
            String.valueOf(RESTUtils.findAvailablePort(5000, 6000))),
        t -> true);

    spyServerConfig = Mockito.spy(serverConfig);

    Mockito.when(
            spyServerConfig.getConfigsWithPrefix(
                AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX))
        .thenReturn(ImmutableMap.of(AuxiliaryServiceManager.AUX_SERVICE_NAMES, ""));
  }

  @BeforeEach
  public void setUp() {
    // Remove rocksdb storage file if exists
    try {
      FileUtils.deleteDirectory(FileUtils.getFile(ROCKS_DB_STORE_PATH));
    } catch (Exception e) {
      // Ignore
    }
    gravitinoServer = new GravitinoServer(spyServerConfig);
  }

  @AfterEach
  public void tearDown() {
    if (gravitinoServer != null) {
      gravitinoServer.stop();
    }
  }

  @Test
  public void testInitialize() {
    gravitinoServer.initialize();
  }

  @Test
  void testConfig() {
    Assertions.assertEquals(
        ROCKS_DB_STORE_PATH, spyServerConfig.get(ENTRY_KV_ROCKSDB_BACKEND_PATH));
  }

  @Test
  public void testStartAndStop() throws Exception {
    gravitinoServer.initialize();
    gravitinoServer.start();
    gravitinoServer.stop();
  }

  @Test
  public void testStartWithoutInitialise() throws Exception {
    assertThrows(RuntimeException.class, () -> gravitinoServer.start());
  }

  @Test
  public void testStopBeforeStart() throws Exception {
    gravitinoServer.stop();
  }

  @Test
  public void testInitializeWithLoadFromFileException() throws Exception {
    ServerConfig config = new ServerConfig();

    // TODO: Exception due to environment variable not set. Is this the right exception?
    assertThrows(IllegalArgumentException.class, () -> config.loadFromFile("config"));
  }
}
