/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server;

import static com.datastrato.graviton.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
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
      "/tmp/graviton_test_server" + UUID.randomUUID().toString().replace("-", "");
  private ServerConfig spyServerConfig;

  @BeforeAll
  void initConfig() {
    String confPath =
        System.getenv("GRAVITON_HOME")
            + File.separator
            + "conf"
            + File.separator
            + "graviton.conf.template";
    ServerConfig serverConfig = GravitonServer.loadConfig(confPath);
    spyServerConfig = Mockito.spy(serverConfig);
    Mockito.when(spyServerConfig.get(ENTRY_KV_ROCKSDB_BACKEND_PATH))
        .thenReturn(ROCKS_DB_STORE_PATH);
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
    assertThrows(GravitonServerException.class, () -> gravitonServer.start());
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
