/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.client.HTTPClient;
import org.apache.gravitino.client.RESTClient;
import org.apache.gravitino.integration.test.util.HttpUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.integration.test.util.KerberosProviderHelper;
import org.apache.gravitino.integration.test.util.OAuthMockDataProvider;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.GravitinoServer;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MiniGravitino is a mini Apache Gravitino server for integration tests. It starts a Gravitino
 * server in the same JVM process.
 */
public class MiniGravitino {

  private static final Logger LOG = LoggerFactory.getLogger(MiniGravitino.class);
  private static final Splitter COMMA = Splitter.on(",").omitEmptyStrings().trimResults();
  private MiniGravitinoContext context;
  private RESTClient restClient;
  private final File mockConfDir;
  private final ServerConfig serverConfig = new ServerConfig();
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private Properties properties;

  private String host;

  private int port;

  public MiniGravitino(MiniGravitinoContext context) throws IOException {
    this.context = context;
    this.mockConfDir = Files.createTempDirectory("MiniGravitino").toFile();
    mockConfDir.mkdirs();
  }

  private void removeAuxRestConfiguration(Properties properties) {
    // Disable Iceberg REST service
    properties.remove(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + AuxiliaryServiceManager.AUX_SERVICE_NAMES);
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

    properties =
        serverConfig.loadPropertiesFromFile(
            new File(ITUtils.joinPath(mockConfDir.getAbsolutePath(), "gravitino.conf")));

    // Disable auxiliary rest service.
    if (context.ignoreAuxRestService) {
      removeAuxRestConfiguration(properties);
      ITUtils.overwriteConfigFile(
          ITUtils.joinPath(mockConfDir.getAbsolutePath(), "gravitino.conf"), properties);
    }

    serverConfig.loadFromProperties(properties);

    // Initialize the REST client
    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, GravitinoServer.WEBSERVER_CONF_PREFIX);
    this.host = jettyServerConfig.getHost();
    this.port = jettyServerConfig.getHttpPort();
    String URI = String.format("http://%s:%d", host, port);

    List<String> authenticators = new ArrayList<>();
    String authenticatorStr = context.customConfig.get(Configs.AUTHENTICATORS.getKey());
    if (authenticatorStr != null) {
      authenticators = COMMA.splitToList(authenticatorStr);
    }

    if (authenticators.contains(AuthenticatorType.OAUTH.name().toLowerCase())) {
      restClient =
          HTTPClient.builder(ImmutableMap.of())
              .uri(URI)
              .withAuthDataProvider(OAuthMockDataProvider.getInstance())
              .build();
    } else if (authenticators.contains(AuthenticatorType.KERBEROS.name().toLowerCase())) {
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

    String url = URI + "/metrics";
    while (System.currentTimeMillis() - beginTime < 1000 * 60 * 3) {
      started = HttpUtils.isHttpServerUp(url);
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

    String url = String.format("http://%s:%d/metrics", host, port);
    while (System.currentTimeMillis() - beginTime < 1000 * 60 * 3) {
      sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
      started = HttpUtils.isHttpServerUp(url);
      if (!started) {
        break;
      }
    }

    restClient.close();
    try {
      FileUtils.deleteDirectory(mockConfDir);
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

    String icebergJarPath = Paths.get("iceberg", "iceberg-rest-server", "build", "libs").toString();
    String icebergConfigPath =
        Paths.get("iceberg", "iceberg-rest-server", "src", "main", "resources").toString();
    customConfigs.put(
        "gravitino.iceberg-rest." + AuxiliaryServiceManager.AUX_SERVICE_CLASSPATH,
        String.join(",", icebergJarPath, icebergConfigPath));

    customConfigs.put(
        "gravitino.iceberg-rest." + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
        String.valueOf(RESTUtils.findAvailablePort(3000, 4000)));
    return customConfigs;
  }

  private Map<String, String> getLanceRestServiceConfigs() throws IOException {
    Map<String, String> customConfigs = new HashMap<>();

    String lanceJarPath = Paths.get("lance", "lance-rest-server", "build", "libs").toString();
    String lanceConfigPath =
        Paths.get("lance", "lance-rest-server", "src", "main", "resources").toString();
    customConfigs.put(
        "gravitino.lance-rest." + AuxiliaryServiceManager.AUX_SERVICE_CLASSPATH,
        String.join(",", lanceJarPath, lanceConfigPath));

    customConfigs.put(
        "gravitino.lance-rest." + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
        String.valueOf(RESTUtils.findAvailablePort(4000, 5000)));
    return customConfigs;
  }

  // Customize the config file
  private void customizeConfigFile(String configTempFileName, String configFileName)
      throws IOException {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(
        GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
        String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));

    configMap.putAll(getIcebergRestServiceConfigs());
    configMap.putAll(getLanceRestServiceConfigs());
    configMap.putAll(context.customConfig);

    ITUtils.rewriteConfigFile(configTempFileName, configFileName, configMap);
  }
}
