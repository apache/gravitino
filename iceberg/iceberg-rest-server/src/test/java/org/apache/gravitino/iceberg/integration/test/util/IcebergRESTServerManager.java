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
package org.apache.gravitino.iceberg.integration.test.util;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.server.GravitinoIcebergRESTServer;
import org.apache.gravitino.integration.test.util.HttpUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IcebergRESTServerManager {

  protected static final Logger LOG = LoggerFactory.getLogger(IcebergRESTServerManager.class);

  protected Map<String, String> customConfigs = new HashMap<>();
  protected Config serverConfig;
  protected String checkUri;

  public abstract Path getConfigDir();

  public abstract Optional<Future<?>> doStartIcebergRESTServer() throws Exception;

  public abstract void doStopIcebergRESTServer();

  public static IcebergRESTServerManager create() {
    String testMode = System.getProperty(ITUtils.TEST_MODE);
    if (ITUtils.EMBEDDED_TEST_MODE.equals(testMode)) {
      return new IcebergRESTServerManagerForEmbedded();
    } else {
      return new IcebergRESTServerManagerForDeploy();
    }
  }

  public void registerCustomConfigs(Map<String, String> configs) {
    customConfigs.putAll(configs);
  }

  public Config getServerConfig() {
    return serverConfig;
  }

  public void startIcebergRESTServer() throws Exception {
    initServerConfig();
    Optional<Future<?>> future = doStartIcebergRESTServer();

    long beginTime = System.currentTimeMillis();
    boolean started = false;

    while (System.currentTimeMillis() - beginTime < 1000 * 60) {
      started = HttpUtils.isHttpServerUp(checkUri);
      if (started || (future.isPresent() && future.get().isDone())) {
        break;
      }
      sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    }
    if (!started) {
      try {
        if (future.isPresent()) {
          future.get().get(1, TimeUnit.SECONDS);
        }
      } catch (Exception e) {
        throw new RuntimeException("GravitinoIcebergRESTServer start failed", e);
      }
      throw new RuntimeException("Can not start GravitinoIcebergRESTServer in one minute");
    }
  }

  public void stopIcebergRESTServer() {
    doStopIcebergRESTServer();
    sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

    long beginTime = System.currentTimeMillis();
    boolean started = true;
    while (System.currentTimeMillis() - beginTime < 1000 * 60) {
      sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
      started = HttpUtils.isHttpServerUp(checkUri);
      if (!started) {
        break;
      }
    }
    if (started) {
      throw new RuntimeException("Can not stop GravitinoIcebergRESTServer in one minute");
    }
  }

  private void customizeConfigFile(String configTempFileName, String configFileName)
      throws IOException {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
        String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));

    configMap.putAll(customConfigs);

    ITUtils.rewriteConfigFile(configTempFileName, configFileName, configMap);
  }

  private void initServerConfig() throws IOException {
    Path configDir = getConfigDir();
    String gravitinoRootDir = System.getenv("GRAVITINO_ROOT_DIR");

    Path configFile = Paths.get(configDir.toString(), GravitinoIcebergRESTServer.CONF_FILE);
    customizeConfigFile(
        Paths.get(gravitinoRootDir, "conf", GravitinoIcebergRESTServer.CONF_FILE + ".template")
            .toString(),
        configFile.toString());
    this.serverConfig = new ServerConfig();
    Properties properties = serverConfig.loadPropertiesFromFile(configFile.toFile());
    serverConfig.loadFromProperties(properties);

    LOG.info("Server config:{}.", serverConfig.getAllConfig());

    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, IcebergConfig.ICEBERG_CONFIG_PREFIX);
    String host = jettyServerConfig.getHost();
    int port = jettyServerConfig.getHttpPort();
    this.checkUri = String.format("http://%s:%d/metrics", host, port);
    LOG.info("Check uri:{}.", checkUri);
  }
}
