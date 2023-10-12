/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server;

import static com.datastrato.graviton.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.graviton.aux.AuxiliaryServiceManager;
import com.datastrato.graviton.rest.RESTUtils;
import com.datastrato.graviton.server.web.JettyServerConfig;
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
public class TestGravitonServer {

  private GravitonServer gravitonServer;
  private final String ROCKS_DB_STORE_PATH =
      "/tmp/graviton_test_server_" + UUID.randomUUID().toString().replace("-", "");
  private ServerConfig spyServerConfig;

  @BeforeAll
  void initConfig() throws IOException {
    ServerConfig serverConfig = new ServerConfig();
    serverConfig.loadFromMap(
        ImmutableMap.of(
            ENTRY_KV_ROCKSDB_BACKEND_PATH.getKey(),
            ROCKS_DB_STORE_PATH,
            GravitonServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
            String.valueOf(RESTUtils.findAvailablePort(5000, 6000))),
        t -> true);

    spyServerConfig = Mockito.spy(serverConfig);

    Mockito.when(
            spyServerConfig.getConfigsWithPrefix(
                AuxiliaryServiceManager.GRAVITON_AUX_SERVICE_PREFIX))
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
    gravitonServer = new GravitonServer(spyServerConfig);
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
  void testConfig() {
    Assertions.assertEquals(
        ROCKS_DB_STORE_PATH, spyServerConfig.get(ENTRY_KV_ROCKSDB_BACKEND_PATH));
  }

  @Test
  public void testStartAndStop() throws Exception {
    gravitonServer.initialize();
    gravitonServer.start();
    gravitonServer.stop();
  }

  @Test
  public void testStartWithoutInitialise() throws Exception {
    assertThrows(RuntimeException.class, () -> gravitonServer.start());
  }

  @Test
  public void testStopBeforeStart() throws Exception {
    gravitonServer.stop();
  }

  @Test
  public void testInitializeWithLoadFromFileException() throws Exception {
    ServerConfig config = new ServerConfig();

    // TODO: Exception due to environment variable not set. Is this the right exception?
    assertThrows(IllegalArgumentException.class, () -> config.loadFromFile("config"));
  }
}
