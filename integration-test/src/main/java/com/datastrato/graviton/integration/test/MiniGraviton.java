/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.test;

import static com.datastrato.graviton.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
import com.datastrato.graviton.client.ErrorHandlers;
import com.datastrato.graviton.client.HTTPClient;
import com.datastrato.graviton.client.RESTClient;
import com.datastrato.graviton.dto.responses.VersionResponse;
import com.datastrato.graviton.exceptions.RESTException;
import com.datastrato.graviton.integration.test.util.ITUtils;
import com.datastrato.graviton.rest.RESTUtils;
import com.datastrato.graviton.server.GravitonServer;
import com.datastrato.graviton.server.ServerConfig;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniGraviton {
  private static final Logger LOG = LoggerFactory.getLogger(MiniGraviton.class);
  private RESTClient restClient;
  private final File mockConfDir;
  private final ServerConfig serverConfig = new ServerConfig();
  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  public MiniGraviton() throws IOException {
    this.mockConfDir = Files.createTempDirectory("MiniGraviton").toFile();
    mockConfDir.mkdirs();
  }

  public void start() throws Exception {
    LOG.info("Staring MiniGraviton up...");

    String gravitonRootDir = System.getenv("GRAVITON_ROOT_DIR");

    // Generate random Graviton Server port and backend storage path, avoiding conflicts
    customizeConfigFile(
        ITUtils.joinDirPath(gravitonRootDir, "conf", "graviton.conf.template"),
        ITUtils.joinDirPath(mockConfDir.getAbsolutePath(), GravitonServer.CONF_FILE));

    Files.copy(
        Paths.get(ITUtils.joinDirPath(gravitonRootDir, "conf", "graviton-env.sh.template")),
        Paths.get(ITUtils.joinDirPath(mockConfDir.getAbsolutePath(), "graviton-env.sh")));

    Properties properties =
        serverConfig.loadPropertiesFromFile(
            new File(ITUtils.joinDirPath(mockConfDir.getAbsolutePath(), "graviton.conf")));
    serverConfig.loadFromProperties(properties);

    // Prepare delete the rocksdb backend storage directory
    try {
      FileUtils.deleteDirectory(FileUtils.getFile(serverConfig.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)));
    } catch (Exception e) {
      // Ignore
    }

    // Initialize the REST client
    String host = serverConfig.get(ServerConfig.WEBSERVER_HOST);
    int port = serverConfig.get(ServerConfig.WEBSERVER_HTTP_PORT);
    String URI = String.format("http://%s:%d", host, port);
    restClient = HTTPClient.builder(ImmutableMap.of()).uri(URI).build();

    Future<?> future =
        executor.submit(
            () -> {
              try {
                GravitonServer.main(
                    new String[] {
                      ITUtils.joinDirPath(mockConfDir.getAbsolutePath(), "graviton.conf")
                    });
              } catch (Exception e) {
                LOG.error("Exception in startup MiniGraviton Server ", e);
                throw new RuntimeException(e);
              }
            });
    long beginTime = System.currentTimeMillis();
    boolean started = false;
    while (System.currentTimeMillis() - beginTime < 1000 * 60 * 3) {
      Thread.sleep(500);
      started = checkIfServerIsRunning();
      if (started || future.isDone()) {
        break;
      }
    }
    if (!started) {
      throw new RuntimeException("Can not start Graviton server");
    }

    LOG.info("MiniGraviton stared.");
  }

  public void stop() throws IOException, InterruptedException {
    LOG.debug("MiniGraviton shutDown...");

    executor.shutdown();
    Thread.sleep(500);
    executor.shutdownNow();

    long beginTime = System.currentTimeMillis();
    boolean started = true;
    while (System.currentTimeMillis() - beginTime < 1000 * 60 * 3) {
      Thread.sleep(500);
      started = checkIfServerIsRunning();
      if (!started) {
        break;
      }
    }

    restClient.close();
    try {
      FileUtils.deleteDirectory(mockConfDir);
      FileUtils.deleteDirectory(FileUtils.getFile(serverConfig.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)));
    } catch (Exception e) {
      // Ignore
    }

    if (started) {
      throw new RuntimeException("Can not stop Graviton server");
    }

    LOG.debug("MiniGraviton terminated.");
  }

  public Config getServerConfig() {
    return serverConfig;
  }

  // Customize the config file
  private void customizeConfigFile(String configTempFileName, String configFileName)
      throws IOException {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(
        ServerConfig.WEBSERVER_HTTP_PORT.getKey(),
        String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    configMap.put(
        Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH.getKey(), "/tmp/graviton-" + UUID.randomUUID());

    Properties props = new Properties();

    try (InputStream inputStream = Files.newInputStream(Paths.get(configTempFileName));
        OutputStream outputStream = Files.newOutputStream(Paths.get(configFileName))) {
      props.load(inputStream);

      for (Map.Entry<String, String> entry : configMap.entrySet()) {
        props.setProperty(entry.getKey(), entry.getValue());
      }

      props.store(outputStream, null);
    } catch (IOException e) {
      LOG.error("Exception in customizeConfigFile ", e);
    }
  }

  private boolean checkIfServerIsRunning() {
    String host = serverConfig.get(ServerConfig.WEBSERVER_HOST);
    int port = serverConfig.get(ServerConfig.WEBSERVER_HTTP_PORT);
    String URI = String.format("http://%s:%d", host, port);
    LOG.info("checkIfServerIsRunning() URI: {}", URI);

    restClient = HTTPClient.builder(ImmutableMap.of()).uri(URI).build();
    VersionResponse response = null;
    try {
      response =
          restClient.get(
              "api/version",
              VersionResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.restErrorHandler());
    } catch (RESTException e) {
      LOG.warn("checkIfServerIsRunning() fails, GravitonServer is not running");
    }
    if (response != null && response.getCode() == 0) {
      return true;
    } else {
      LOG.warn("checkIfServerIsRunning() fails, GravitonServer is not running");
    }

    return false;
  }
}
