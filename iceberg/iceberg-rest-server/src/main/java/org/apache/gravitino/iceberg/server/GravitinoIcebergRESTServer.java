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
package org.apache.gravitino.iceberg.server;

import com.google.common.base.Preconditions;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.iceberg.RESTService;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authentication.ServerAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoIcebergRESTServer {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoIcebergRESTServer.class);

  public static final String CONF_FILE = "gravitino-iceberg-rest-server.conf";

  private final ServerConfig serverConfig;

  private RESTService icebergRESTService;
  private GravitinoEnv gravitinoEnv;

  public GravitinoIcebergRESTServer(ServerConfig config) {
    this.serverConfig = config;
    this.gravitinoEnv = GravitinoEnv.getInstance();
    this.icebergRESTService = new RESTService();
  }

  private void initialize() {
    gravitinoEnv.initializeBaseComponents(serverConfig);
    icebergRESTService.serviceInit(
        serverConfig.getConfigsWithPrefix(IcebergConfig.ICEBERG_CONFIG_PREFIX));
    ServerAuthenticator.getInstance().initialize(serverConfig);
  }

  private void start() {
    gravitinoEnv.start();
    icebergRESTService.serviceStart();
  }

  private void join() {
    icebergRESTService.join();
  }

  private void stop() throws Exception {
    icebergRESTService.serviceStop();
    LOG.info("Gravitino Iceberg REST service stopped");
  }

  public static void main(String[] args) {
    LOG.info("Starting Gravitino Iceberg REST Server");
    String confPath = System.getenv("GRAVITINO_TEST") == null ? "" : args[0];
    ServerConfig serverConfig = ServerConfig.loadConfig(confPath, CONF_FILE);
    Preconditions.checkArgument(
        !serverConfig.get(Configs.ENABLE_AUTHORIZATION),
        "Iceberg REST server standalone mode doesn't support authorization.");
    GravitinoIcebergRESTServer icebergRESTServer = new GravitinoIcebergRESTServer(serverConfig);
    icebergRESTServer.initialize();

    try {
      icebergRESTServer.start();
    } catch (Exception e) {
      LOG.error("Error while running jettyServer", e);
      System.exit(-1);
    }
    LOG.info("Done, Gravitino Iceberg REST server started.");

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    // Register some clean-up tasks that need to be done before shutting down
                    Thread.sleep(serverConfig.get(ServerConfig.SERVER_SHUTDOWN_TIMEOUT));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.error("Interrupted exception:", e);
                  } catch (Exception e) {
                    LOG.error("Error while running clean-up tasks in shutdown hook", e);
                  }
                }));
    icebergRESTServer.join();

    LOG.info("Shutting down Gravitino Iceberg REST Server ... ");
    try {
      icebergRESTServer.stop();
      LOG.info("Gravitino Iceberg REST Server has shut down.");
    } catch (Exception e) {
      LOG.error("Error while stopping Gravitino Iceberg REST Server", e);
    }
  }
}
