/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration;

import static com.datastrato.graviton.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
import com.datastrato.graviton.client.ErrorHandlers;
import com.datastrato.graviton.client.HTTPClient;
import com.datastrato.graviton.client.RESTClient;
import com.datastrato.graviton.dto.responses.VersionResponse;
import com.datastrato.graviton.exceptions.RESTException;
import com.datastrato.graviton.integration.util.EnvironmentVariable;
import com.datastrato.graviton.integration.util.ITUtils;
import com.datastrato.graviton.server.GravitonServer;
import com.datastrato.graviton.server.ServerConfig;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniGraviton {
  private static final Logger LOG = LoggerFactory.getLogger(MiniGraviton.class);
  private RESTClient restClient;
  private final File confDir;
  private final Config serverConfig = new ServerConfig();
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final Runnable SERVER =
      new Runnable() {
        @Override
        public void run() {
          try {
            GravitonServer.main(new String[] {""});
          } catch (Exception e) {
            LOG.error("Exception in startup MiniGraviton Server ", e);
            throw new RuntimeException(e);
          }
        }
      };

  public MiniGraviton() {
    this.confDir =
        new File(ITUtils.joinDirPath(".", "build", UUID.randomUUID().toString(), "conf"));
    this.confDir.mkdirs();
  }

  public void start() throws Exception {
    LOG.info("Staring MiniGraviton up...");

    // copy the resources files to a temp folder
    String confFileName = ITUtils.joinDirPath(confDir.getAbsolutePath(), "graviton.conf");
    String gravitonRootDir = System.getenv("GRAVITON_ROOT_DIR");
    // Generate random Graviton Server port and backend storage path, avoiding conflicts
    customizeConfigFile(
        ITUtils.joinDirPath(gravitonRootDir, "conf", "graviton.conf.template"), confFileName);

    Files.copy(
        Paths.get(ITUtils.joinDirPath(gravitonRootDir, "conf", "graviton-env.sh.template")),
        Paths.get(ITUtils.joinDirPath(confDir.getAbsolutePath(), "graviton-env.sh")));

    EnvironmentVariable.injectEnv("GRAVITON_HOME", System.getenv("GRAVITON_ROOT_DIR"));
    EnvironmentVariable.injectEnv("GRAVITON_CONF_DIR", confDir.getAbsolutePath());
    EnvironmentVariable.injectEnv("GRAVITON_TEST", "true");
    serverConfig.loadFromFile(GravitonServer.CONF_FILE);

    // Prepare delete the rocksdb backend storage directory
    try {
      FileUtils.deleteDirectory(FileUtils.getFile(serverConfig.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)));
    } catch (Exception e) {
      // Ignore
    }

    // Prepare the REST client
    String host = serverConfig.get(ServerConfig.WEBSERVER_HOST);
    int port = serverConfig.get(ServerConfig.WEBSERVER_HTTP_PORT);
    String URI = String.format("http://%s:%d", host, port);
    restClient = HTTPClient.builder(ImmutableMap.of()).uri(URI).build();

    executor.submit(SERVER);
    long beginTime = System.currentTimeMillis();
    boolean started = false;
    while (System.currentTimeMillis() - beginTime < 1000 * 60 * 3) {
      Thread.sleep(500);
      started = checkIfServerIsRunning();
      if (started) {
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
      FileUtils.deleteDirectory(confDir);
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
    configMap.put(ServerConfig.WEBSERVER_HTTP_PORT.getKey(), String.valueOf(findAvailablePort()));
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

  private int findAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      LOG.error("Exception in findAvailablePort ", e);
      throw e;
    }
  }

  private boolean checkIfServerIsRunning() {
    String host = serverConfig.get(ServerConfig.WEBSERVER_HOST);
    int port = serverConfig.get(ServerConfig.WEBSERVER_HTTP_PORT);
    String URI = String.format("http://%s:%d", host, port);
    LOG.info("checkIfServerIsRunning() URI: {}", URI);

    boolean isRunning = false;
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
      isRunning = true;
    } else {
      LOG.warn("checkIfServerIsRunning() fails, GravitonServer is not running");
    }
    return isRunning;
  }
}
