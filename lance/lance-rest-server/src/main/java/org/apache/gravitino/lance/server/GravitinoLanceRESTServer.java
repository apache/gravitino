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
package org.apache.gravitino.lance.server;

import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.lance.LanceRESTService;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authentication.ServerAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bootstrap entry point for the Lance REST facade. */
public class GravitinoLanceRESTServer {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoLanceRESTServer.class);

  public static final String CONF_FILE = "gravitino-lance-rest-server.conf";

  private final Config serverConfig;

  private LanceRESTService lanceRESTService;
  private GravitinoEnv gravitinoEnv;

  public GravitinoLanceRESTServer(Config config) {
    this.serverConfig = config;
    this.gravitinoEnv = GravitinoEnv.getInstance();
    this.lanceRESTService = new LanceRESTService();
  }

  private void initialize() {
    gravitinoEnv.initializeBaseComponents(serverConfig);
    lanceRESTService.serviceInit(
        serverConfig.getConfigsWithPrefix(LanceConfig.LANCE_CONFIG_PREFIX));
    ServerAuthenticator.getInstance().initialize(serverConfig);
  }

  private void start() {
    gravitinoEnv.start();
    lanceRESTService.serviceStart();
  }

  private void join() {
    lanceRESTService.join();
  }

  private void stop() throws Exception {
    lanceRESTService.serviceStop();
    LOG.info("Gravitino Lance REST service stopped");
  }

  public static void main(String[] args) {
    LOG.info("Starting Gravitino Lance REST Server");
    String confPath = System.getenv("GRAVITINO_TEST") == null ? "" : args[0];
    ServerConfig serverConfig = ServerConfig.loadConfig(confPath, CONF_FILE);
    GravitinoLanceRESTServer lanceRESTServer = new GravitinoLanceRESTServer(serverConfig);
    lanceRESTServer.initialize();

    try {
      lanceRESTServer.start();
    } catch (Exception e) {
      LOG.error("Error while running lance REST server", e);
      System.exit(-1);
    }
    LOG.info("Done, Gravitino Lance REST server started.");

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    Thread.sleep(serverConfig.get(ServerConfig.SERVER_SHUTDOWN_TIMEOUT));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.error("Interrupted exception:", e);
                  } catch (Exception e) {
                    LOG.error("Error while running clean-up tasks in shutdown hook", e);
                  }
                }));
    lanceRESTServer.join();

    LOG.info("Shutting down Gravitino Lance REST Server ... ");
    try {
      lanceRESTServer.stop();
      LOG.info("Gravitino Lance REST Server has shut down.");
    } catch (Exception e) {
      LOG.error("Error while stopping Gravitino Lance REST Server", e);
    }
  }
}
