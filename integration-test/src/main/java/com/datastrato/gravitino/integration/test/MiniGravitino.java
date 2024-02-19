/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test;

import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.auth.AuthenticatorType;
import com.datastrato.gravitino.auxiliary.AuxiliaryServiceManager;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergRESTService;
import com.datastrato.gravitino.client.ErrorHandlers;
import com.datastrato.gravitino.client.HTTPClient;
import com.datastrato.gravitino.client.RESTClient;
import com.datastrato.gravitino.dto.responses.VersionResponse;
import com.datastrato.gravitino.exceptions.RESTException;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.integration.test.util.KerberosProviderHelper;
import com.datastrato.gravitino.integration.test.util.OAuthMockDataProvider;
import com.datastrato.gravitino.rest.RESTUtils;
import com.datastrato.gravitino.server.GravitinoServer;
import com.datastrato.gravitino.server.ServerConfig;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
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
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniGravitino {
  private static final Logger LOG = LoggerFactory.getLogger(MiniGravitino.class);
  private MiniGravitinoContext context;
  private RESTClient restClient;
  private final File mockConfDir;
  private final ServerConfig serverConfig = new ServerConfig();
  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  private String host;

  private int port;

  public MiniGravitino(MiniGravitinoContext context) throws IOException {
    this.context = context;
    this.mockConfDir = Files.createTempDirectory("MiniGravitino").toFile();
    mockConfDir.mkdirs();
  }

  public void start() throws Exception {
    LOG.info("Staring MiniGravitino up...");

    String gravitinoRootDir = System.getenv("GRAVITINO_ROOT_DIR");

    // Generate random Gravitino Server port and backend storage path, avoiding conflicts
    customizeConfigFile(
        ITUtils.joinPath(gravitinoRootDir, "conf", "gravitino.conf.template"),
        ITUtils.joinPath(mockConfDir.getAbsolutePath(), GravitinoServer.CONF_FILE));

    Files.copy(
        Paths.get(ITUtils.joinPath(gravitinoRootDir, "conf", "gravitino-env.sh.template")),
        Paths.get(ITUtils.joinPath(mockConfDir.getAbsolutePath(), "gravitino-env.sh")));

    Properties properties =
        serverConfig.loadPropertiesFromFile(
            new File(ITUtils.joinPath(mockConfDir.getAbsolutePath(), "gravitino.conf")));
    serverConfig.loadFromProperties(properties);

    // Prepare delete the rocksdb backend storage directory
    try {
      FileUtils.deleteDirectory(FileUtils.getFile(serverConfig.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)));
    } catch (Exception e) {
      // Ignore
    }

    // Initialize the REST client
    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, GravitinoServer.WEBSERVER_CONF_PREFIX);
    this.host = jettyServerConfig.getHost();
    this.port = jettyServerConfig.getHttpPort();
    String URI = String.format("http://%s:%d", host, port);
    if (AuthenticatorType.OAUTH
        .name()
        .toLowerCase()
        .equals(context.customConfig.get(Configs.AUTHENTICATOR.getKey()))) {
      restClient =
          HTTPClient.builder(ImmutableMap.of())
              .uri(URI)
              .withAuthDataProvider(OAuthMockDataProvider.getInstance())
              .build();
    } else if (AuthenticatorType.KERBEROS
        .name()
        .toLowerCase()
        .equals(context.customConfig.get(Configs.AUTHENTICATOR.getKey()))) {
      restClient =
          HTTPClient.builder(ImmutableMap.of())
              .uri(URI)
              .withAuthDataProvider(KerberosProviderHelper.getProvider())
              .build();
    } else {
      restClient = HTTPClient.builder(ImmutableMap.of()).uri(URI).build();
    }

    Future<?> future =
        executor.submit(
            () -> {
              try {
                GravitinoServer.main(
                    new String[] {
                      ITUtils.joinPath(mockConfDir.getAbsolutePath(), "gravitino.conf")
                    });
              } catch (Exception e) {
                LOG.error("Exception in startup MiniGravitino Server ", e);
                throw new RuntimeException(e);
              }
            });
    long beginTime = System.currentTimeMillis();
    boolean started = false;
    while (System.currentTimeMillis() - beginTime < 1000 * 60 * 3) {
      started = checkIfServerIsRunning();
      if (started || future.isDone()) {
        break;
      }
      sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    }
    if (!started) {
      try {
        future.get(5, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new RuntimeException("Gravitino server start failed", e);
      }
      throw new RuntimeException("Can not start Gravitino server");
    }

    LOG.info("MiniGravitino stared.");
  }

  public void stop() throws IOException, InterruptedException {
    LOG.debug("MiniGravitino shutDown...");

    executor.shutdown();
    sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    executor.shutdownNow();

    long beginTime = System.currentTimeMillis();
    boolean started = true;
    while (System.currentTimeMillis() - beginTime < 1000 * 60 * 3) {
      sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
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
      throw new RuntimeException("Can not stop Gravitino server");
    }

    LOG.debug("MiniGravitino terminated.");
  }

  public Config getServerConfig() {
    return serverConfig;
  }

  Map<String, String> getIcebergRestServiceConfigs() throws IOException {
    Map<String, String> customConfigs = new HashMap<>();

    String icebergJarPath =
        Paths.get("catalogs", "catalog-lakehouse-iceberg", "build", "libs").toString();
    String icebergConfigPath =
        Paths.get("catalogs", "catalog-lakehouse-iceberg", "src", "main", "resources").toString();
    customConfigs.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + AuxiliaryServiceManager.AUX_SERVICE_CLASSPATH,
        String.join(",", icebergJarPath, icebergConfigPath));

    customConfigs.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
        String.valueOf(RESTUtils.findAvailablePort(3000, 4000)));
    return customConfigs;
  }

  // Customize the config file
  private void customizeConfigFile(String configTempFileName, String configFileName)
      throws IOException {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(
        GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
        String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    configMap.put(
        Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH.getKey(), "/tmp/gravitino-" + UUID.randomUUID());

    configMap.putAll(getIcebergRestServiceConfigs());
    configMap.putAll(context.customConfig);

    ITUtils.rewriteConfigFile(configTempFileName, configFileName, configMap);
  }

  private boolean checkIfServerIsRunning() {
    String URI = String.format("http://%s:%d", host, port);
    LOG.info("checkIfServerIsRunning() URI: {}", URI);

    VersionResponse response = null;
    try {
      response =
          restClient.get(
              "api/version",
              VersionResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.restErrorHandler());
    } catch (RESTException e) {
      LOG.warn("checkIfServerIsRunning() fails, GravitinoServer is not running {}", e.getMessage());
      return false;
    }
    if (response != null && response.getCode() == 0) {
      return true;
    } else {
      LOG.warn("checkIfServerIsRunning() fails, GravitinoServer is not running");
      return false;
    }
  }
}
